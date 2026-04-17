(ns bitool.endpoint
  (:require [bitool.db :as db]
            [bitool.logic :as logic]
            [clojure.string :as string]
            [cheshire.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [taoensso.telemere :as tel])
  (:import javax.crypto.Mac
           javax.crypto.spec.SecretKeySpec))

;; Dynamic endpoint registry
;; Structure: {graph-id {node-id {:method :get :path "/users/:id" :config {...}}}}
(defonce endpoint-registry (atom {}))

;; Rate limiter state: {[graph-id node-id key] [timestamp-ms ...]}
(defonce rl-state (atom {}))

(defn- normalize-route-path
  "Normalize user/imported route formats into matcher-friendly form:
   - trims whitespace
   - ensures leading '/'
   - converts OpenAPI '{id}' segments to ':id'"
  [path]
  (let [p (-> (or path "")
              string/trim
              (string/replace #"\{([^}/]+)\}" ":$1"))]
    (cond
      (string/blank? p) ""
      (string/starts-with? p "/") p
      :else (str "/" p))))

(defn hmac-sha256
  "Compute HMAC-SHA256 of body-str with secret, return lowercase hex string."
  [secret body-str]
  (let [key-spec (SecretKeySpec. (.getBytes secret "UTF-8") "HmacSHA256")
        mac      (doto (Mac/getInstance "HmacSHA256") (.init key-spec))
        bytes    (.doFinal mac (.getBytes body-str "UTF-8"))]
    (apply str (map #(format "%02x" (bit-and % 0xff)) bytes))))

(defn verify-webhook-signature
  "Returns [valid? request'] where request' has :body restored as a ByteArrayInputStream
   so downstream handlers can still read the body after signature verification.
   Returns [true request] immediately when no secret is configured."
  [secret-value secret-header request]
  (let [secret (string/trim (or secret-value ""))]
    (if (empty? secret)
      [true request]  ;; no secret configured — skip verification
      (let [raw-bytes  (.readAllBytes (:body request))
            raw-body   (String. raw-bytes "UTF-8")
            expected   (hmac-sha256 secret raw-body)
            sig-hdr    (get-in request [:headers (string/lower-case (or secret-header ""))] "")
            provided   (string/replace-first sig-hdr #"(?i)^sha256=" "")
            ;; Restore body stream so downstream middleware/handlers can read it
            restored   (assoc request :body (java.io.ByteArrayInputStream. raw-bytes))]
        [(= expected provided) restored]))))

(defn register-endpoint!
  "Register an endpoint definition for a given graph.
   Handles both Ep nodes (route_path / http_method) and
   Wh nodes (webhook_path, always POST)."
  [graph-id node-id ep-config]
  (let [btype  (:btype ep-config)
        path   (normalize-route-path
                (if (= btype "Wh")
                  (:webhook_path ep-config)
                  (:route_path ep-config)))
        method (if (= btype "Wh")
                 :post
                 (keyword (string/lower-case (or (:http_method ep-config) "get"))))]
    (if (string/blank? path)
      (do
        (tel/log! :warn (str "Skipped " btype " registration for graph " graph-id
                             " node " node-id " due to blank path"))
        nil)
      (let [entry {:method method :path path :config ep-config}]
        (swap! endpoint-registry assoc-in [graph-id node-id] entry)
        (tel/log! :info (str "Registered " btype ": " (name method) " " path
                             " for graph " graph-id " node " node-id))
        entry))))

(defn unregister-endpoint!
  "Remove an endpoint registration."
  [graph-id node-id]
  (swap! endpoint-registry update graph-id dissoc node-id))

(defn restore-endpoints!
  "On server startup, scan all graphs and re-register every Ep and Wh node."
  []
  (let [gids (try (db/list-graph-ids) (catch Exception _ []))]
    (doseq [gid gids]
      (try
        (let [g     (db/getGraph gid)
              nodes (:n g)]
          (doseq [[nid {:keys [na]}] nodes]
            (when (some #{(:btype na)} ["Ep" "Wh"])
              (register-endpoint! gid nid na))))
        (catch Exception e
          (tel/log! :warn (str "restore-endpoints!: skipped graph " gid " — " (.getMessage e))))))))

(defn deploy-graph-endpoints!
  "Register all Ep/Wh nodes from the given graph and return deploy metadata
   including testable dynamic URLs."
  [graph-id]
  (let [g        (db/getGraph graph-id)
        deployed (->> (:n g)
                      (reduce
                       (fn [acc [nid {:keys [na]}]]
                         (let [btype (:btype na)]
                           (if (some #{btype} ["Ep" "Wh"])
                             (if-let [entry (register-endpoint! graph-id nid na)]
                               (conj acc {:node_id       nid
                                          :btype         btype
                                          :method        (-> (:method entry) name string/upper-case)
                                          :route_path    (if (= btype "Wh") (:webhook_path na) (:route_path na))
                                          :deployed_path (:path entry)
                                          :url           (str "/api/v1/ep/" graph-id (:path entry))})
                               acc)
                             acc)))
                       [])
                      (sort-by (juxt :method :deployed_path))
                      vec)]
    {:graph_id  graph-id
     :base_path (str "/api/v1/ep/" graph-id)
     :count     (count deployed)
     :endpoints deployed}))

(defn flat-params
  "Flatten {:path {:id \"1\"} :query {:limit \"10\"} :body {}} into a single string-keyed map."
  [params]
  (merge (into {} (map (fn [[k v]] [(name k) v]) (:path params)))
         (into {} (map (fn [[k v]] [(name k) v]) (:query params)))
         (into {} (map (fn [[k v]] [(name k) v]) (:body params)))))

(defn apply-template
  "Map flat params through Rb template [{:output_key \"k\" :source_column \"col\"}].
   Returns a map of output keys to values. Columns not found in params are omitted."
  [template flat]
  (reduce (fn [acc {:keys [output_key source_column]}]
            (if (and output_key source_column (contains? flat source_column))
              (assoc acc output_key (get flat source_column))
              acc))
          {}
          template))

(defn- find-node-by-btype
  "BFS from start-id to find the first downstream node with the given btype.
   Returns the node's :na data map or nil."
  [g start-id btype]
  (loop [queue [start-id] visited #{}]
    (when-let [nid (first queue)]
      (let [na       (get-in g [:n nid :na])
            children (keys (get-in g [:n nid :e]))]
        (if (= (:btype na) btype)
          na
          (let [unvisited (remove visited children)]
            (recur (concat (rest queue) unvisited)
                   (conj visited nid))))))))

(defn- find-nodes-by-btypes
  "BFS from start-id to collect all downstream nodes whose btype is in btypes, in visit order."
  [g start-id btypes]
  (loop [queue [start-id]
         idx 0
         visited #{}
         matches []]
    (if (>= idx (count queue))
      matches
      (let [nid      (nth queue idx)
            na       (get-in g [:n nid :na])
            children (keys (get-in g [:n nid :e]))
            visited' (conj visited nid)
            queue'   (reduce (fn [acc child]
                               (if (contains? visited' child)
                                 acc
                                 (conj acc child)))
                             queue
                             children)
            matches' (if (contains? btypes (:btype na))
                       (conj matches (assoc na :id nid))
                       matches)]
        (recur queue' (inc idx) visited' matches')))))

(defn find-rb-node
  "BFS from ep-node-id to find the first downstream Rb node.
   Returns its data map or nil."
  [g ep-node-id]
  (find-node-by-btype g ep-node-id "Rb"))

(defn- bearer-token
  "Extract Bearer token from Authorization header value, or return the raw value."
  [header-val]
  (if (string/starts-with? (string/lower-case (or header-val "")) "bearer ")
    (subs header-val 7)
    (or header-val "")))

(defn- validate-auth
  "Check the request headers against an Au node config.
   Returns [valid? error-msg]. Always returns [true nil] when no Au node found."
  [au-node request]
  (if-not au-node
    [true nil]
    (let [auth-type    (string/lower-case (or (:auth_type au-node) ""))
          token-header (string/lower-case (or (:token_header au-node) "authorization"))
          secret       (string/trim (or (:secret au-node) ""))
          header-val   (get-in request [:headers token-header] "")]
      (cond
        (empty? secret)
        [true nil]

        (= auth-type "api-key")
        [(= header-val secret) "Invalid API key"]

        :else
        [(= (bearer-token header-val) secret) "Unauthorized"]))))

(defn- rl-key
  "Derive the rate-limit bucket key from the request based on key_type config.
   key_type may be 'ip', 'global', or 'header:<header-name>' (as saved by the UI)."
  [rl-node request]
  (let [kt (or (:key_type rl-node) "ip")]
    (cond
      (= kt "ip")
      (or (get-in request [:headers "x-forwarded-for"])
          (:remote-addr request)
          "unknown")

      (= kt "global")
      "global"

      (string/starts-with? kt "header:")
      (let [hdr-name (string/lower-case (subs kt (count "header:")))]
        (get-in request [:headers hdr-name] "anonymous"))

      :else
      (or (:remote-addr request) "unknown"))))

(defn- check-rate-limit!
  "Sliding-window rate limit check. Returns [allowed? retry-after-secs].
   Mutates rl-state atom to record this request timestamp."
  [graph-id node-id rl-node request]
  (let [max-req  (or (:max_requests rl-node) 100)
        window   (or (:window_seconds rl-node) 60)
        burst    (or (:burst rl-node) 0)
        limit    (+ max-req burst)
        k        [graph-id node-id (rl-key rl-node request)]
        now-ms   (System/currentTimeMillis)
        cutoff   (- now-ms (* window 1000))
        new-ts   (swap! rl-state update k
                        (fn [ts]
                          (conj (filterv #(> % cutoff) (or ts [])) now-ms)))
        count    (count (get new-ts k))]
    (if (<= count limit)
      [true nil]
      [false window])))

(defn- parameterise-template
  "Convert {{key}} placeholders to JDBC positional parameters (?).
   Returns [parameterised-sql [value1 value2 ...]] where values are
   taken from flat in placeholder order. Unresolved placeholders use nil."
  [sql-str flat]
  (let [placeholders (re-seq #"\{\{(\w+)\}\}" sql-str)
        values       (mapv (fn [[_ k]] (get flat k nil)) placeholders)
        param-sql    (string/replace sql-str #"\{\{\w+\}\}" "?")]
    [param-sql values]))

(defn- execute-dx-node
  "Execute a Dx node's SQL against the registered connection using
   parameterised queries (no string interpolation — injection-safe).
   Returns merged flat params: for 'single' mode, merges first result row;
   for 'multi' mode, adds 'rows' key with all rows as string maps."
  [dx-node flat]
  (let [conn-id      (:connection_id dx-node)
        sql-template (or (:sql_template dx-node) "")
        operation    (string/upper-case (or (:operation dx-node) "SELECT"))
        result-mode  (or (:result_mode dx-node) "single")]
    (when (and conn-id (seq (string/trim sql-template)))
      (try
        (let [opts               (db/get-opts conn-id false)
              [param-sql values] (parameterise-template sql-template flat)
              stmt               (into [param-sql] values)]
          (if (= operation "SELECT")
            (let [rows (jdbc/execute! opts stmt {:builder-fn rs/as-unqualified-lower-maps})
                  ->str-map #(into {} (map (fn [[k v]] [(name k) (str v)]) %))]
              (if (= result-mode "single")
                (merge flat (->str-map (first rows)))
                (assoc flat "rows" (mapv ->str-map rows))))
            (let [result (jdbc/execute-one! opts stmt)]
              (assoc flat "affected_rows" (str (get result :next.jdbc/update-count 0))))))
        (catch Exception e
          (tel/log! :warn (str "execute-dx-node: SQL failed — " (.getMessage e)))
          flat)))))

(defn- execute-transform-nodes
  [g start-id flat]
  (reduce (fn [acc node]
            (case (:btype node)
              "Dx" (or (execute-dx-node node acc) acc)
              "C"  (logic/execute-conditional-node node acc)
              "Fu" (logic/execute-logic-node node acc)
              acc))
          flat
          (find-nodes-by-btypes g start-id #{"Dx" "C" "Fu"})))

(defn- validate-vd-node
  "Run Vd rules against flat params. Returns nil on success.
   Throws 422 ExceptionInfo with a map of field->message on the first set of failures."
  [vd-node flat]
  (letfn [(key->str [k]
            (cond
              (keyword? k) (name k)
              (string? k) k
              (nil? k) ""
              :else (str k)))
          (flat-value [m field]
            (let [field-name (key->str field)
                  missing    (Object.)
                  direct     (get m field-name missing)]
              (if (not (identical? direct missing))
                direct
                (or (some (fn [[k v]]
                            (when (= (string/lower-case (key->str k))
                                     (string/lower-case field-name))
                              v))
                          m)
                    ""))))]
  (when vd-node
    (let [rules   (or (:rules vd-node) [])
          errors  (reduce
                    (fn [acc {:keys [field rule value message]}]
                      (let [field-name (key->str field)
                            v      (flat-value flat field)
                            v-str  (str v)
                            v-trim (string/trim v-str)
                            msg    (or (and (seq (str message)) (str message))
                                       (str field " failed rule: " rule))
                            fail?  (case (str rule)
                                     "required"   (string/blank? v-str)
                                     "min"        (let [n (try (Double/parseDouble v-trim)
                                                               (catch Exception _ nil))]
                                                    (or (nil? n)
                                                        (< n (Double/parseDouble (str value)))))
                                     "max"        (let [n (try (Double/parseDouble v-trim)
                                                               (catch Exception _ nil))]
                                                    (or (nil? n)
                                                        (> n (Double/parseDouble (str value)))))
                                     "min-length" (< (count v-str) (Integer/parseInt (str value)))
                                     "max-length" (> (count v-str) (Integer/parseInt (str value)))
                                     "regex"      (not (re-find (re-pattern (str value)) v-str))
                                     "one-of"     (not (contains? (set (map string/trim (string/split (str value) #",")))
                                                                   v-trim))
                                     "type"       (case (string/lower-case (str value))
                                                    "integer" (nil? (re-matches #"-?\d+" v-trim))
                                                    "number"  (nil? (re-matches #"-?\d+(\.\d+)?" v-trim))
                                                    "boolean" (not (contains? #{"true" "false"} (string/lower-case v-trim)))
                                                    false)
                                     false)]
                        (if fail? (assoc acc field-name msg) acc)))
                    {}
                    rules)]
      (when (seq errors)
        (throw (ex-info "Validation failed"
                        {:status 422 :error "Validation failed" :errors errors})))))))

(defn- cors-headers
  "Build CORS response headers from a Cr node config and the request Origin header.
   Returns an empty map when no Cr node is configured."
  [cr-node request]
  (if-not cr-node
    {}
    (let [origins     (or (:allowed_origins cr-node) [])
          methods     (or (:allowed_methods cr-node) [])
          headers     (or (:allowed_headers cr-node) [])
          credentials (boolean (:allow_credentials cr-node))
          max-age     (str (or (:max_age cr-node) 86400))
          req-origin  (get-in request [:headers "origin"] "")
          wildcard?      (some #{"*"} origins)
          ;; Wildcard + credentials is forbidden by the CORS spec — degrade to exact match only
          allowed-origin (cond
                           (and wildcard? (not credentials)) "*"
                           (some #{req-origin} origins)     req-origin
                           :else                             nil)]
      (if allowed-origin
        (cond-> {"Access-Control-Allow-Origin"  allowed-origin
                 "Access-Control-Allow-Methods" (string/join ", " methods)
                 "Access-Control-Allow-Headers" (string/join ", " headers)
                 "Access-Control-Max-Age"        max-age}
          credentials (assoc "Access-Control-Allow-Credentials" "true"))
        {}))))

(defn- publish-ev-node!
  "Fire-and-forget: POST flat params as JSON to the Ev node's broker URL.
   Errors are logged and swallowed — response is never affected."
  [ev-node flat]
  (when ev-node
    (future
      (try
        (let [broker  (string/trim (or (:broker_url ev-node) ""))
              topic   (string/trim (or (:topic ev-node) ""))
              url     (if (and (seq broker) (seq topic))
                        (str broker "/" topic)
                        broker)
              payload (json/generate-string flat)]
          (when (seq url)
            (let [conn (-> url java.net.URL. .openConnection)]
              (doto conn
                (.setRequestMethod "POST")
                (.setDoOutput true)
                (.setRequestProperty "Content-Type" "application/json"))
              (with-open [out (.getOutputStream conn)]
                (.write out (.getBytes payload "UTF-8")))
              (.getResponseCode conn))))
        (catch Exception e
          (tel/log! :warn (str "publish-ev-node!: failed — " (.getMessage e))))))))

(defn execute-graph
  "Run the graph pipeline with the given input parameters.
   Applies the Rb (Response Builder) node config: template field mappings, status code,
   and extra headers. Falls back to echoing params when no Rb node is found.
   request is the raw Ring request map used for auth header validation."
  [graph-id node-id params request]
  (let [g       (db/getGraph graph-id)
        au-node (find-node-by-btype g node-id "Au")
        [auth-ok? auth-err] (validate-auth au-node request)
        _       (when-not auth-ok?
                  (throw (ex-info auth-err {:status 401 :error auth-err})))
        rl-node (find-node-by-btype g node-id "Rl")
        [rl-ok? retry-after] (if rl-node
                               (check-rate-limit! graph-id node-id rl-node request)
                               [true nil])
        _       (when-not rl-ok?
                  (throw (ex-info "Rate limit exceeded"
                                  {:status  429
                                   :headers {"Retry-After" (str retry-after)}
                                   :error   "Rate limit exceeded"})))
        vd-node (find-node-by-btype g node-id "Vd")
        _       (validate-vd-node vd-node (flat-params params))
        flat    (execute-transform-nodes g node-id (flat-params params))
        ev-node (find-node-by-btype g node-id "Ev")
        _       (publish-ev-node! ev-node flat)
        cr-node (find-node-by-btype g node-id "Cr")
        rb-node (find-rb-node g node-id)]
    (if rb-node
      (let [template    (or (:template rb-node) [])
            status-code (Integer/parseInt (or (:status_code rb-node) "200"))
            headers-raw (or (:headers rb-node) "")
            extra-hdrs  (when (seq (string/trim headers-raw))
                          (try (json/parse-string headers-raw)
                               (catch Exception _ nil)))
            response-type (or (:response_type rb-node) "json")
            content-type  (case response-type
                            "xml"  "application/xml"
                            "csv"  "text/csv"
                            "text" "text/plain"
                            "application/json")
            data          (if (seq template)
                            (let [mapped (apply-template template flat)]
                              (if (seq mapped) mapped flat))
                            flat)
            body          (case response-type
                            "xml"  (str "<response>"
                                        (string/join (map (fn [[k v]] (str "<" k ">" v "</" k ">")) data))
                                        "</response>")
                            "csv"  (str (string/join "," (keys data)) "\n"
                                        (string/join "," (vals data)))
                            "text" (string/join "\n" (map (fn [[k v]] (str k "=" v)) data))
                            (json/generate-string data))
            response      {:status  status-code
                           :headers (merge {"Content-Type" content-type}
                                           (cors-headers cr-node request)
                                           (or extra-hdrs {}))
                           :body    body}]
        response)
      ;; No Rb node — echo params as fallback
      {:status  200
       :headers (merge {"Content-Type" "application/json"}
                       (cors-headers cr-node request))
       :body    (json/generate-string {:echo flat})})))

(defn path-matches?
  "Check if a URI matches a route pattern with :param segments.
   e.g. /users/42 matches /users/:id"
  [pattern uri]
  (let [pattern-parts (string/split pattern #"/")
        uri-parts (string/split uri #"/")]
    (and (= (count pattern-parts) (count uri-parts))
         (every? true?
           (map (fn [p u]
                  (or (string/starts-with? p ":")
                      (= p u)))
                pattern-parts uri-parts)))))

(defn extract-path-params
  "Extract path parameter values from a URI given a route pattern.
   e.g. pattern=/users/:id uri=/users/42 => {:id \"42\"}"
  [pattern uri]
  (let [pattern-parts (string/split pattern #"/")
        uri-parts (string/split uri #"/")]
    (into {}
      (keep (fn [[p u]]
              (when (string/starts-with? p ":")
                [(keyword (subs p 1)) u]))
            (map vector pattern-parts uri-parts)))))

(defn build-handler
  "Build a Ring handler for a registered endpoint."
  [graph-id node-id route-path]
  (fn [request]
    (try
      (let [ep-uri       (or (:bitool/ep-path request) (:uri request))
            path-params  (extract-path-params route-path ep-uri)
            query-params (:query-params request)
            body-params  (:params request)
            merged       {:path  path-params
                          :query query-params
                          :body  body-params}]
        (execute-graph graph-id node-id merged request))
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)
              body (cond-> {:error (.getMessage e)}
                     (:errors data) (assoc :errors (:errors data)))]
          {:status  (or (:status data) 500)
           :headers (merge {"Content-Type" "application/json"}
                           (or (:headers data) {}))
           :body    (json/generate-string body)})))))

(defn- dispatch-endpoint
  "Dispatch a matched endpoint entry, running Wh signature verification first."
  [gid nid ep request ep-path]
  (let [config  (:config ep)
        btype   (:btype config)]
    (if (= btype "Wh")
      (let [routed-request (assoc request :bitool/ep-path ep-path)
            [valid? req'] (verify-webhook-signature
                            (:secret_value config)
                            (:secret_header config)
                            routed-request)]
        (if valid?
          ((build-handler gid nid (:path ep)) req')
          {:status  401
           :headers {"Content-Type" "application/json"}
           :body    (json/generate-string {:error "Invalid webhook signature"})}))
      ((build-handler gid nid (:path ep)) (assoc request :bitool/ep-path ep-path)))))

(defn dynamic-endpoint-handler
  "A catch-all handler that checks the endpoint registry and dispatches.
   All dynamic endpoints are served under /api/v1/ep/<graph-id>/<route-path>."
  [request]
  (let [uri    (:uri request)
        method (:request-method request)]
    (when (string/starts-with? uri "/api/v1/ep/")
      (let [rest-uri  (subs uri (count "/api/v1/ep/"))
            slash-idx (string/index-of rest-uri "/")
            gid-str   (if slash-idx (subs rest-uri 0 slash-idx) rest-uri)
            ep-path   (if slash-idx (subs rest-uri slash-idx) "/")]
        (try
          (let [gid (Integer/parseInt gid-str)]
            (when-let [nodes (get @endpoint-registry gid)]
              (some (fn [[nid ep]]
                      (when (and (= method (:method ep))
                                 (path-matches? (:path ep) ep-path))
                        (dispatch-endpoint gid nid ep request ep-path)))
                    nodes)))
          (catch NumberFormatException _ nil))))))
