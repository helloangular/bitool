(ns bitool.connector.api
  (:require
    [clojure.core.async :as async
     :refer [chan timeout go-loop >! <! alt! alt!! close! thread pipeline pipeline-blocking]]
    [clj-http.client :as http]
    [clj-http.cookies :as cookies]
    [cheshire.core :as json]
    [clj-http.conn-mgr :as conn]
    [bitool.utils :refer :all]
    [bitool.connector.paginate :as paginate]
    [bitool.connector.auth :as auth]
    [bitool.api.jsontf :as tf]
    [bitool.macros :refer :all]
    [taoensso.timbre :as log]))

;; =============================================================================
;;                     HTTP + AUTH (merged from original)
;; =============================================================================

(def default-user-agent
  "DevLake-Connector/1.0 (+https://your-domain.example)")

(def default-headers
  {"User-Agent"       default-user-agent
   "Accept"           "application/json"
   "Accept-Language"  "en-US,en;q=0.9"
   "X-Requested-With" "XMLHttpRequest"})

(def cookie-store (cookies/cookie-store))

(defn- json-content-type? [headers]
  (when-let [ct (some-> (or (get headers :content-type)
                            (get headers "content-type"))
                        str)]
    (boolean (re-find #"(?i)application\/json" ct))))

(defn- try-parse-json
  "Parse string body into Clojure data when Content-Type is JSON or body starts with { / [.
   Otherwise return the string body as-is."
  [headers body-str]
  (try
    (when (and (string? body-str)
               (or (json-content-type? headers)
                   (and (not (empty? body-str))
                        (#{\{ \[} (first body-str)))))
      (json/parse-string body-str true))
    (catch Throwable _
      body-str)))

(defn- resolve-auth
  "Accepts:
     - map {:type ...}
     - atom of that map
     - 0-arg fn returning that map
   Returns {:auth map :source :map|:atom|:fn|:none}."
  [a]
  (cond
    (nil? a)                               {:auth nil :source :none}
    (map? a)                               {:auth a   :source :map}
    (instance? clojure.lang.Atom a)        {:auth @a  :source :atom}
    (fn? a)  (let [m (try (a) (catch Throwable e (log/error e "auth provider failed") nil))]
               {:auth m :source :fn})
    :else (do (log/warn "Unsupported :auth type" (type a))
              {:auth nil :source :unknown})))

(defn- merge-auth
  "Combine headers/query with auth-derived headers/query using bitool.connector.auth/build-auth."
  [headers q auth-map ctx]
  (let [{ah :headers aq :query} (if auth-map (auth/build-auth auth-map ctx) {:headers {} :query {}})]
    {:headers (merge headers (or ah {}))
     :query   (merge q       (or aq {}))}))

;; --- Shared HTTP connection pool -------------------------------------------

(defonce ^:private pooled-conn-mgr
  (conn/make-reusable-conn-manager
    {:timeout            5000    ;; idle connection eviction (ms)
     :threads            64      ;; max total connections
     :default-per-route  16}))   ;; max per host

(def pooled-http-opts
  {:connection-manager pooled-conn-mgr
   :decompress-body?   true
   :accept-encoding    [:gzip :deflate]  
   :accept             "application/json"
   :throw-exceptions   false
   :redirect-strategy  :lax
   :follow-redirects   true
   :ignore-unknown-host? false})


(defn do-request
  "Blocking HTTP GET using pooled connections + gzip compression.
   Safe for async threads. Handles 401/403 refresh once if auth has :refresh-fn."
  [{:keys [url base-headers query-params auth] :as ctx}]
  (let [auth-param auth
        {:keys [auth source]} (resolve-auth auth-param)
        {:keys [headers query]} (merge-auth
                                 (merge default-headers (or base-headers {}))
                                 (or query-params {})
                                 auth ctx)
        base-opts (merge
                    pooled-http-opts
                    {:headers headers
                     :query-params query
                     :as :string
                     :cookie-store cookie-store})
        http-> (fn [opts]
                 (try
                   (http/get url opts)
                   (catch Exception e
                     (log/error e "HTTP request failed (transport)")
                     {:status nil :headers {} :body (str e)})))]

    (let [resp1 (http-> base-opts)
          status1 (:status resp1)
          headers1 (:headers resp1)
          body1 (:body resp1)]
      (if (and (#{401 403} status1) (:refresh-fn auth))
        ;; refresh once
        (let [new-auth (try ((:refresh-fn auth))
                            (catch Throwable e
                              (log/error e "auth refresh-fn failed")
                              nil))]
          ;; Only reset! if caller passed an atom
          (when (and new-auth (instance? clojure.lang.IAtom auth-param))
            (reset! auth-param new-auth))
          (let [{:keys [headers query]} (merge-auth
                                         (merge default-headers (or base-headers {}))
                                         (or query-params {})
                                         new-auth ctx)
                resp2 (http-> (assoc base-opts
                                :headers headers
                                :query-params query))]
            {:status  (:status resp2)
             :headers (:headers resp2)
             :body    (try-parse-json (:headers resp2) (:body resp2))}))
        ;; normal path
        {:status  status1
         :headers headers1
         :body    (try-parse-json headers1 body1)}))))

;; =============================================================================
;;                     Small Pure Helpers (modular producer)
;; =============================================================================

(defn- inverse-map [m] (into {} (map (fn [[k v]] [v k]) m)))

(defn- api-query-from-state
  "Translate internal paginator keys -> API query keys for the next request."
  [state out-key-map base-query]
  (reduce (fn [q [internal-k api-k]]
            (if (contains? state internal-k)
              (assoc q api-k (get state internal-k))
              q))
          (or base-query {})
          out-key-map))

(defn- apply-state-key-map
  "Translate API body fields -> internal paginator keys (keeps state in one dialect)."
  [body state-key-map]
  (reduce (fn [m [api-k internal-k]]
            (let [v (get body api-k)]
              (cond-> m (some? v) (assoc internal-k v))))
          {} state-key-map))

(defn- derive-offset-metrics
  "Derive page/total-pages etc. from internal offset/limit/total + body.isLast."
  [{:keys [offset limit total]} body]
  (let [off    (long (or offset 0))
        lim    (long (max 1 (or limit 50)))
        tot    (some-> total long)
        page   (long (inc (quot off lim)))
        tpages (when tot (long (Math/ceil (/ (double tot) lim))))
        islast (boolean (or (:isLast body)
                            (and tot (>= (+ off lim) tot))))]
    {:offset off, :limit lim, :total tot, :page page, :total-pages tpages, :is-last islast}))

(defn- classify-error [resp body]
  (let [st (:status resp)
        ct (or (get-in resp [:headers :content-type])
               (get-in resp [:headers "content-type"]))
        non-json? (not (map? body))
        preview   (when (string? body) (subs body 0 (min 200 (count body))))]
    (cond
      (nil? st)  {:type :transport :status nil :preview preview}                 ;; NEW
      (= st 429) {:type :rate-limit :status st
                  :retry-after (or (get-in resp [:headers "retry-after"])
                                   (get-in resp [:headers :retry-after]))
                  :preview preview}
      (= st 401) {:type :unauthorized :status st :preview preview}
      (= st 403) {:type :forbidden    :status st :preview preview}
      (<= 500 st){:type :server-error :status st :preview preview}
      non-json?  {:type :non-json     :status st :content-type ct :preview preview}
      :else nil)))

;; in make-envelope (bitool.connector.api)
(defn- make-envelope
  [{:keys [state body resp metrics]}]
  (merge
    {:body body :response resp :state state
     :http-status (:status resp)}                        ;; ← add this line
    metrics
    (when (and (= (:type state) :link-header)
               (nil? (:page metrics)))
      (when-let [lnk (get-in resp [:headers "link"])]
        {:page (paginate/extract-page lnk)}))))


;; =============================================================================
;;                     Side-effect helpers
;; =============================================================================

(defn- do-get!
  "Run do-request on a real thread; returns a channel of response."
  [{:keys [url headers auth query]}]
  (thread (do-request {:url url :base-headers headers :query-params query :auth auth})))

(defn- emit-page! [{:keys [pages-ch envelope body page->items-fn]}]
  (let [env* (if page->items-fn
               (try (assoc envelope :items (vec (or (page->items-fn body) [])))
                    (catch Exception e
                      (log/error e "page->items-fn failed; emitting without :items")
                      envelope))
               envelope)]
    (clojure.core.async/put! pages-ch env* (fn [_] nil))))   ;; <- put!, safe outside go


(defn- emit-error! [errors-ch where url query err]
  (async/put! errors-ch (merge err {:where where :url url :query query}) (fn [_] nil)))

(defn- maybe-sleep!
  "Rate-limit sleep: prefer Retry-After seconds if present; else per-page-ms."
  [{:keys [retry-after per-page-ms]}]
  (let [ms (cond
             retry-after (try (* 1000 (Long/parseLong (str retry-after)))
                              (catch Exception _ 0))
             (pos? (long (or per-page-ms 0))) (long per-page-ms)
             :else 0)]
    (when (pos? ms) (Thread/sleep ms))))

;; =============================================================================
;;                     Producer (modular go-loop)
;; =============================================================================

(defn- next-url+query
  "Decide the next request URL and query from current state."
  [{:keys [base-url endpoint state first? pagination query-builder out-key-map]}]
  (let [url   (or (:next-url state) (str base-url endpoint))
        query (cond
                first? (or query-builder {})
                (= pagination :link-header) {}
                :else (api-query-from-state state out-key-map query-builder))]
    {:url url :query query}))

(defn- compute-state'
  "Update internal state using API body (state-key-map) and attach metrics for :offset."
  [{:keys [state body state-key-map]}]
  (let [state1  (merge state (apply-state-key-map (or body {}) (or state-key-map {})))
        metrics (when (= (:type state1) :offset)
                  (derive-offset-metrics state1 (or body {})))]
    {:state' state1 :metrics metrics}))

(defn- decide-next
  "Ask the paginator for the next page. Returns {:state-next :url-next} or nil."
  [{:keys [state' resp base-url endpoint]}]
  (when-let [np (paginate/next-page (assoc state' :response resp))]
    {:state-next (merge (dissoc state' :response :next-url) np)
     :url-next   (or (:next-url np) (str base-url endpoint))}))

;; finish!
(defn- finish! [{:keys [pages-ch errors-ch resp state reason]}]
  (let [msg {:stop-reason reason
             :http-status (:status resp)
             :response    resp
             :state       state}]
    ;; Try a non-blocking offer of the terminal page; close regardless.
    (clojure.core.async/offer! pages-ch msg)
    (clojure.core.async/close! pages-ch)
    (clojure.core.async/close! errors-ch)))

(defn- run-producer!
  [{:keys [base-url endpoint headers auth pagination query-builder initial-state
           state-key-map out-key-map page->items-fn
           pages-ch errors-ch stop-ch rate-limit]}]
  (let [out-map (or out-key-map (inverse-map (or state-key-map {})))]
    (go-loop [state  (merge {:type pagination} (or initial-state {}))
              first? true
              url    (str base-url endpoint)]
      (if (async/poll! stop-ch)
        (finish! {:pages-ch pages-ch :errors-ch errors-ch
                  :resp nil :state state :reason :cancelled})
        (let [{:keys [url query]} (next-url+query {:base-url base-url
                                                   :endpoint endpoint
                                                   :state state
                                                   :first? first?
                                                   :pagination pagination
                                                   :query-builder query-builder
                                                   :out-key-map out-map})
              resp (<! (do-get! {:url url :headers headers :auth auth :query query}))
              body (:body resp)
              err  (classify-error resp body)]
          (when err (emit-error! errors-ch :fetch url query err))

          (cond
            ;; Terminal error conditions
            (and err (#{:transport :unauthorized :forbidden :rate-limit :server-error :non-json} (:type err)))
            (finish! {:pages-ch pages-ch :errors-ch errors-ch
                      :resp resp :state state :reason (:type err)})

            :else
            (let [{:keys [state' metrics]} (compute-state' {:state state :body body :state-key-map state-key-map})
                  envelope (make-envelope {:state state' :body body :resp resp :metrics metrics})]
              ;; emit current page
              (emit-page! {:pages-ch pages-ch :envelope envelope :body body :page->items-fn page->items-fn})

		    (log/debug "state' to paginator:" (select-keys state' [:type :offset :limit :total :is-last :page]))

              ;; ask paginator for next
              (if-let [{:keys [state-next url-next]} (decide-next {:state' state' :resp resp
                                                                   :base-url base-url :endpoint endpoint})]
                (do
                  (maybe-sleep! {:retry-after (or (get-in resp [:headers "retry-after"])
                                                  (get-in resp [:headers :retry-after]))
                                 :per-page-ms (:per-page-ms rate-limit)})
                  (recur state-next false url-next))
                (finish! {:pages-ch pages-ch :errors-ch errors-ch
                          :resp resp :state state' :reason :eof})))))))))

;; =============================================================================
;;                     Public API (tiny wrapper)
;; =============================================================================

(defn fetch-paged-async
  "Async, paginated fetcher with optional processing workers.
   Returns:
     - without :process-fn -> {:pages ch, :errors ch, :cancel fn}
     - with    :process-fn -> {:pages ch, :results ch, :errors ch, :cancel fn}"
  [{:keys [base-url endpoint headers auth pagination query-builder initial-state
           state-key-map out-key-map page->items-fn process-fn
           worker-count pages-buffer results-buffer blocking? rate-limit]
    :or   {worker-count 4 pages-buffer 500 results-buffer 500 blocking? true
           rate-limit {:per-page-ms 0}}}]
  (let [pages-ch  (chan pages-buffer)
        errors-ch (chan 10)
        stop-ch   (chan 1)]
    ;; producer
    (run-producer! {:base-url base-url :endpoint endpoint :headers headers :auth auth
                    :pagination pagination :query-builder query-builder :initial-state initial-state
                    :state-key-map state-key-map :out-key-map out-key-map :page->items-fn page->items-fn
                    :pages-ch pages-ch :errors-ch errors-ch :stop-ch stop-ch :rate-limit rate-limit})
    ;; optional worker pool
    (if (nil? process-fn)
      {:pages  pages-ch
       :errors errors-ch
       :cancel (fn [] (async/offer! stop-ch true))}
      (let [results-ch (chan results-buffer)
            xf (comp
                 (map (fn [{:keys [body page state response items] :as env}]
                        (try
                          (let [items* (or items (when page->items-fn
                                                   (vec (or (page->items-fn body) []))))]
                            (process-fn (cond-> env (some? items*) (assoc :items items*))))
                          (catch Exception e
                            (log/error e "process-fn failed for page" page)
                            nil))))
                 (remove nil?))]
        (if blocking?
          (pipeline-blocking worker-count results-ch xf pages-ch)
          (pipeline          worker-count results-ch xf pages-ch))
        {:pages   pages-ch
         :results results-ch
         :errors  errors-ch
         :cancel  (fn [] (async/offer! stop-ch true))}))))


(def jira-auth
  (atom {:type :atlassian-basic
         :email "you@example.com"            ;; <-- change me
         :api-token "REDACTED_GITHUB_TOKEN"             ;; <-- change me
         :refresh-fn (fn []
                       ;; If your token rotates, return a fresh one here.
                       {:type :atlassian-basic
                        :email "you@example.com"
                        :api-token "REDACTED_GITHUB_TOKEN"})}))

(def issue-path->col
  {"$.key"                               :key
   "$.fields.summary"                    :summary
   "$.fields.created"                    :created
   "$.fields.assignee.displayName"       :assignee
   "$.fields.labels[]"                   :labels}) ;; arrays are fine

(defn path->col[path_coll]
      (zipmap path_coll (map #(path->name %) path_coll)))
(comment
(defn extract-rows-from-items
  "Given one page envelope, turn its :items into a vector of rows.
   Each 'item' is a single JSON object."
  [{:keys [items page total-pages http-status]}]
  ;; Convert every item to ONE row using :per-context mode
  (let [rows (mapv (fn [item]
                     (-> (tf/rows-from-json item issue-path->col {:row-mode :per-context})
                         first))                     ; rows-from-json returns a seq; take the row
                   items)]
    ;; return a page-shaped result for the results channel
    {:page page
     :http-status http-status
     :row-count (count rows)
     :rows rows}))
)

(defn extract-rows-from-items
  "Given one page envelope, turn its :items into a vector of rows.
   Each 'item' is a single JSON object.

   2-arity: path-spec can be:
     - a vector of JSON paths
     - a {path -> col} map
     - nil (falls back to issue-path->col)"
  ([{:keys [items page http-status] :as env} path-spec]
   (let [mapping (cond
                   (map? path-spec)        path-spec
                   (sequential? path-spec) (path->col path-spec)
                   (nil? path-spec)        issue-path->col
                   :else                   issue-path->col)
         rows    (mapv (fn [item]
                         (-> (tf/rows-from-json item mapping {:row-mode :per-context})
                             first))
                       items)
        ;;_ (prn-v mapping)
        ]
     {:page       page
      :http-status http-status
      :row-count  (count rows)
      :rows       rows})))

(defn make-extract-rows-fn
  "Return a process-fn suitable for fetch-paged-async.
   arg can be a vector of paths or a mapping {path -> col}."
  [path-spec]
  (fn [env]
    (extract-rows-from-items env path-spec)))

(defn run-jira-pagination-test
  "Options:
   :base-url, :endpoint, :jql, :per-page-ms, :timeout-ms, :auth, :max-pages
   Returns {:pages N :results M :errors K :stop-reason kw :http-status s}."
  [{:keys [base-url endpoint jql per-page-ms timeout-ms auth max-pages col-paths]
    :or   {base-url "https://issues.apache.org/jira"
           endpoint "/rest/api/2/search"
           jql      "project=HADOOP ORDER BY created DESC"
           per-page-ms 0
           timeout-ms nil
           auth nil
           max-pages 2}}]

  (let [process-fn (make-extract-rows-fn col-paths)
        {:keys [pages results errors cancel]}
        (fetch-paged-async
          {:base-url base-url
           :endpoint endpoint
           :pagination :offset
           :auth auth
           :query-builder (cond-> {:jql jql :startAt 0 :maxResults 100}
                            ;; optional: shrink fields for speed
                            true (assoc :fields "key,summary,created,assignee,labels" :expand ""))
           :initial-state {:offset 0 :limit 100}
           :state-key-map {:startAt :offset :maxResults :limit :total :total}
           :rate-limit {:per-page-ms per-page-ms}
           :page->items-fn :issues
           :process-fn process-fn 
           :worker-count 8
           :pages-buffer 256
           :results-buffer 512})]

    (let [done          (async/chan 1)
          page-count    (atom 0)     ;; processed pages count (results)
          all-rows      (atom [])
          error-count   (atom 0)
          stop-reason*  (atom nil)
          stop-status*  (atom nil)
          terminal?     (fn [t] (contains? #{:transport :unauthorized :forbidden :rate-limit :server-error :non-json} t))]

      ;; Drain RESULTS; respect :max-pages
(comment
      (go-loop []
        (if-let [r (<! results)]
          (do
            (swap! page-count inc)
            (when (and max-pages (>= @page-count max-pages))
              (println (format "Reached max-pages limit (%d), stopping..." max-pages))
              (reset! stop-reason* :max-pages-reached)
              (>! done :max-pages))
            (when (nil? @stop-reason*) (recur)))
          (>! done :results-closed)))
)
;; Drain channels: accumulate all rows from all pages into one vector

      ;; collect extracted rows from :results
      (go-loop []
        (if-let [{:keys [rows page http-status]} (<! results)]
          (do
            (swap! page-count inc)
            (swap! all-rows into rows)
            (when (or (= page 1) (zero? (mod (long page) 25)))
              (println (format "status=%s processed page %s" http-status page)))
 ;; stop early if page limit reached
      (when (and max-pages (>= @page-count max-pages))
        (println (format "Reached max-pages limit (%d), cancelling..." max-pages))
        (reset! stop-reason* :max-pages-reached)
        (>! done :max-pages)
        (cancel))
      (when (nil? @stop-reason*)
        (recur)))
    (async/>! done :closed)))

      ;; Drain ERRORS and capture first terminal reason
      (go-loop []
        (if-let [e (<! errors)]
          (do
            (swap! error-count inc)
            (println "ERROR:" e)
            (when (and (nil? @stop-reason*) (terminal? (:type e)))
              (reset! stop-reason* (:type e))
              (reset! stop-status* (:status e))
              (>! done :terminal-error)))
          (>! done :errors-closed)))

      ;; Wait for terminal error, results close, max-pages, or timeout
      (let [ports (cond-> [done] timeout-ms (conj (async/timeout timeout-ms)))
            [val _] (async/alts!! ports :priority true)]
        (cancel)
        (cond
          (= val :results-closed) (when (nil? @stop-reason*) (reset! stop-reason* :eof))
          (= val :max-pages)      nil
          (nil? val)              (reset! stop-reason* :timeout))
        {:pages @page-count
         :results @page-count
         :rows @all-rows
         :errors @error-count
         :stop-reason @stop-reason*
         :http-status @stop-status*}))))

;; ================ REST API example ==================

(comment
(def res
  (run-jira-pagination-test
    {:base-url "https://issues.apache.org/jira"
     :max-pages 1
     :timeout-ms 30000
     :col-paths ["$.key"
                 "$.fields.summary"
                 "$.fields.created"
                 "$.fields.assignee.displayName"
                 "$.fields.labels[]"]}))

(defn test-api[] 
    (doseq [row (:rows res)]
          (println row)))

)
(comment

this gives below example rows

#_ "{key HADOOP-19652, fields_summary Fix dependency exclusion list of hadoop-client-runtime., fields_created 00, fields_assignee_displayName Cheng Pan, fields_labels[] pull-request-available}"
#_ "{key HADOOP-19651, fields_summary Upgrade libopenssl to 3.5.2-1 needed for rsync, fields_created 2025-08-15T19:19:04.000+0000, fields_assignee_displayName Gautham Banasandra, fields_labels[] pull-request-available}"

============================
api node example

2
{:na
   {:y 140,
    :specification_url
    "https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json",
    :truncate false,
    :create_table false,
    :name "api-connection",
    :endpoints
    ({:endpoint_url
      "rest/api/3/issuetypescreenscheme/{issueTypeScreenSchemeId}/project",
      :selected_nodes ["$.isLast" "$.maxResults"],
      :table_name "project",
      :create_table false,
      :truncate false}),
    :c 123,
    :x 323,
    :api_name "hh",
    :btype "Ap"},
   :e
   {3     =============================// edge to target
    {:specification_url
     "https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json",
     :truncate false,
     :create_table false,
     :endpoint_url
     "rest/api/3/issuetypescreenscheme/{issueTypeScreenSchemeId}/project",
     :id 2,
     :api_name "hh",
     :table_name "project",
     :selected_nodes ["$.isLast" "$.maxResults"]}}}

=====================================================

api-node edge { endpoint selected_nodes } api/run-jira-pagination-test insert-rows 

)
;; ================ Graph QL ==========================

(defn do-gql-request
  "Blocking GraphQL POST using pooled connections + gzip compression.
   Safe for async threads. Reuses auth + error handling logic from do-request.
   Expects:
     {:url      \"https://api.github.com/graphql\"
      :headers  {...}                 ;; extra headers (User-Agent, etc)
      :auth     <same as do-request>  ;; optional auth map/atom/fn
      :query    \"query ($cursor: String, $pageSize: Int!) { ... }\"
      :variables {:cursor nil :pageSize 50}}"
  [{:keys [url headers auth query variables] :as ctx}]
  (let [auth-param auth
        {:keys [auth source]} (resolve-auth auth-param)
        {:keys [headers _]}   (merge-auth
                               (merge default-headers (or headers {}))
                               {}
                               auth ctx)
        base-opts (merge
                    pooled-http-opts
                    {:headers headers
                     :as :string
                     :cookie-store cookie-store
                     :content-type :json
                     :accept :json})
        body-json (json/encode {:query     query
                                :variables (or variables {})})
        http-> (fn [opts]
                 (try
                   (http/post url (assoc opts :body body-json))
                   (catch Exception e
                     (log/error e "GraphQL request failed (transport)")
                     {:status nil :headers {} :body (str e)})))]
    (let [resp1 (http-> base-opts)
          status1  (:status resp1)
          headers1 (:headers resp1)
          body1    (:body resp1)]
      (if (and (#{401 403} status1) (:refresh-fn auth))
        ;; refresh once
        (let [new-auth (try ((:refresh-fn auth))
                            (catch Throwable e
                              (log/error e "auth refresh-fn failed (GraphQL)")
                              nil))]
          (when (and new-auth (instance? clojure.lang.IAtom auth-param))
            (reset! auth-param new-auth))
          (let [{:keys [headers _]} (merge-auth
                                     (merge default-headers (or headers {}))
                                     {}
                                     new-auth ctx)
                resp2 (http-> (assoc base-opts
                                :headers headers))]
            {:status  (:status resp2)
             :headers (:headers resp2)
             :body    (try-parse-json (:headers resp2) (:body resp2))}))
        ;; normal path
        {:status  status1
         :headers headers1
         :body    (try-parse-json headers1 body1)}))))

(defn- parse-iso-odt
  "Parse ISO-8601 timestamp (e.g. 2024-01-02T03:04:05Z) into java.time.OffsetDateTime.
   Returns nil if s is nil or invalid."
  [^String s]
  (when (some? s)
    (try
      (java.time.OffsetDateTime/parse s)
      (catch Exception _
        nil))))

(defn gql-connection-page->rows
  "Turn ONE GraphQL page into {:rows [...], :has-next-page? bool, :end-cursor s, :stop? bool}.
   Arguments:
     body            - parsed JSON body (keywordized)
     connection-path - vector path to the connection map, e.g. [:data :repository :issues]
     nodes-key       - key under connection that holds items (usually :nodes)
     updated-at-key  - key on each node with updated timestamp (e.g. :updatedAt)
     last-updated    - cutoff timestamp string (ISO-8601) from DevLake state; may be nil
     path->col       - JSONPath mapping for rows-from-json (same style as REST)"
  [{:keys [body connection-path nodes-key updated-at-key last-updated path->col]}]
  (let [conn          (get-in body connection-path)
        nodes         (vec (get conn (or nodes-key :nodes)))
        page-info     (:pageInfo conn)
        has-next?     (boolean (:hasNextPage page-info))
        end-cursor    (:endCursor page-info)
        cutoff-ts     (parse-iso-odt last-updated)
        ;; fold over nodes, stop when we see first <= cutoff
        {:keys [rows stop?]}
        (reduce
          (fn [{:keys [rows stop?] :as acc} node]
            (if stop?
              (reduced acc)
              (let [ua-str (get node (or updated-at-key :updatedAt))
                    ua-ts  (parse-iso-odt ua-str)
                    newer? (or (nil? cutoff-ts)
                               (and ua-ts
                                    (pos? (.compareTo ua-ts cutoff-ts))))]
                (if newer?
                  ;; build one row from node using your JSON TF
                  (let [row (-> (tf/rows-from-json
                                  node
                                  path->col
                                  {:row-mode :per-context})
                                first)]
                    (update acc :rows conj row))
                  ;; node is older or equal -> signal global stop
                  (assoc acc :stop? true)))))
          {:rows [] :stop? false}
          nodes)]
    {:rows           rows
     :has-next-page? has-next?
     :end-cursor     end-cursor
     :stop?          stop?}))


(defn run-gql-updated-at-pagination
  "Run a GraphQL cursor-based pagination loop with updatedAt cutoff.

   Options map:
     :url              - GraphQL endpoint, e.g. \"https://api.github.com/graphql\"
     :headers          - extra headers (Authorization, etc)
     :auth             - same auth type you already use (map/atom/fn)
     :query            - GraphQL query string with $cursor and $pageSize variables
     :base-variables   - map of base vars (e.g. {:owner \"foo\" :name \"bar\"})
     :connection-path  - path to connection (e.g. [:data :repository :pullRequests])
     :nodes-key        - key with item list (default :nodes)
     :updated-at-key   - key on node (default :updatedAt)
     :last-updated     - ISO-8601 cutoff string from state; nil for full sync
     :path->col        - JSONPath -> col mapping for rows-from-json
     :page-size        - Int page size (default 50)
     :max-pages        - hard cap on pages (default 100)

   Returns:
     {:pages       N
      :per-page    [{:page 1 :rows [...]} {:page 2 :rows [...]} ...]
      :rows        [all rows flattened]
      :stop-reason :cutoff|:no-more|:max-pages|:error
      :last-http   <status code or nil>}"
  [{:keys [url headers auth query base-variables connection-path nodes-key
           updated-at-key last-updated path->col page-size max-pages]
    :or   {page-size 50 max-pages 100}}]
  (loop [cursor      nil
         page-num    1
         acc-pages   []
         stop-reason nil
         last-http   nil]
    (if stop-reason
      ;; done
      {:pages     (count acc-pages)
       :per-page  acc-pages
       :rows      (vec (mapcat :rows acc-pages))
       :stop-reason stop-reason
       :last-http last-http}
      (if (> page-num max-pages)
        ;; hit hard page cap
        {:pages       (count acc-pages)
         :per-page    acc-pages
         :rows        (vec (mapcat :rows acc-pages))
         :stop-reason :max-pages
         :last-http   last-http}
        ;; fetch next page
        (let [vars      (cond-> (merge base-variables {:pageSize page-size})
                          cursor (assoc :cursor cursor))
              resp      (do-gql-request {:url url
                                         :headers headers
                                         :auth auth
                                         :query query
                                         :variables vars})
              status    (:status resp)
              body      (:body resp)
              ;; classify simple hard errors
              hard-err? (or (nil? status)
                            (>= status 500)
                            (= status 401)
                            (= status 403))
              ;; page extraction
              {:keys [rows has-next-page? end-cursor stop?]}
              (when-not hard-err?
                (gql-connection-page->rows
                  {:body           body
                   :connection-path connection-path
                   :nodes-key      (or nodes-key :nodes)
                   :updated-at-key (or updated-at-key :updatedAt)
                   :last-updated   last-updated
                   :path->col      path->col}))
              page-entry {:page page-num
                          :http-status status
                          :rows rows}]
          (cond
            hard-err?
            (recur cursor
                   page-num
                   (conj acc-pages page-entry)
                   :error
                   status)

            stop?
            ;; hit updatedAt cutoff on this page
            (recur cursor
                   (inc page-num)
                   (conj acc-pages page-entry)
                   :cutoff
                   status)

            (not has-next-page?)
            ;; no more pages from server
            (recur cursor
                   (inc page-num)
                   (conj acc-pages page-entry)
                   :no-more
                   status)

            :else
            ;; continue with next cursor
            (recur end-cursor
                   (inc page-num)
                   (conj acc-pages page-entry)
                   nil
                   status)))))))

(def github-gql
  "query ($owner: String!, $name: String!, $cursor: String, $pageSize: Int!) {
     repository(owner: $owner, name: $name) {
       pullRequests(
         first: $pageSize
         after: $cursor
         states: [OPEN, CLOSED, MERGED]
         orderBy: {field: UPDATED_AT, direction: DESC}
       ) {
         nodes {
           number
           title
           state
           updatedAt
           createdAt
           url
         }
         pageInfo {
           hasNextPage
           endCursor
         }
       }
     }
   }")

(def pr-path->col
  {"$.number"    :number
   "$.title"     :title
   "$.state"     :state
   "$.updatedAt" :updated_at
   "$.createdAt" :created_at
   "$.url"       :url})

(def github-auth
  (atom
    {:type :github-token
     :token "REDACTED_GITHUB_TOKEN"      ;; <-- your GitHub PAT
     :refresh-fn (fn []
                   ;; If you ever rotate your PAT, return a new one here
                   {:type :github-token
                    :token "REDACTED_GITHUB_TOKEN"})}))


(defn demo-github-prs []
  (let [result (run-gql-updated-at-pagination
    {:url             "https://api.github.com/graphql"
     :headers         {:authorization "Bearer REDACTED_GITHUB_TOKEN"
                       :user-agent    "bitool"}
     :auth            nil ;; github-auth      ;; or reuse your auth atom if you want
     :query           github-gql
     :base-variables  {:owner "apache" :name "incubator-devlake"}
     :connection-path [:data :repository :pullRequests]
     :nodes-key       :nodes
     :updated-at-key  :updatedAt
     :last-updated    nil ;; "2024-01-01T00:00:00Z"   ;; from DevLake state; nil for full sync
     :path->col       pr-path->col
     :page-size       50
     :max-pages       1})]
(println "=== Rows ===")
    (doseq [row (:rows result)]
      (clojure.pprint/pprint row))
    nil))

(defn demo-github-clj-prs []
  (run-gql-updated-at-pagination
    {:url             "https://api.github.com/graphql"
     :headers         {:authorization "Bearer REDACTED_GITHUB_TOKEN"
                       :user-agent    "bitool"}
     :auth            nil
     :query           github-gql
     :base-variables  {:owner "clojure" :name "clojure"}  ; <- Changed this
     :connection-path [:data :repository :pullRequests]
     :nodes-key       :nodes
     :updated-at-key  :updatedAt
     :last-updated    nil   ; <- Changed to nil for full sync
     :path->col       pr-path->col
     :page-size       50
     :max-pages       20}))
