(ns bitool.connector.paginate
  (:require [clojure.string :as str]
            [bitool.connector.config :as config]
            [taoensso.timbre :as log]))

;; --- Offset / Page-based Pagination ---
(defn offset-next
  "Offset-based pagination: returns next query params if records remain.
   Nil-safe: if :total is missing, keep advancing by :limit unless :is-last true."
  [{:keys [offset limit total is-last]}]
  (let [offset (long (or offset 0))
        limit  (long (max 1 (or limit 50)))]
    (cond
      (true? is-last)                        ;; server said it's the last page
      nil

      (number? total)                        ;; we know the total → compare
      (let [next-offset (+ offset limit)]
        (when (< next-offset total)
          {:offset next-offset :limit limit}))

      :else                                  ;; unknown total → advance conservatively
      {:offset (+ offset limit) :limit limit})))


(defn page-next
  [{:keys [page total-pages page-size]}]
  (let [page (or page 1)]
    (cond
      (nil? total-pages) {:page (inc page) :page-size page-size}
      (< page total-pages) {:page (inc page) :page-size page-size}
      :else nil)))

;; --- Cursor-based Pagination ---
(defn cursor-next
  "Cursor-based pagination: next cursor from API response token."
  [{:keys [last-cursor]}]
  (when last-cursor
    {:cursor last-cursor}))

;; --- Token-based Pagination ---
(defn token-next
  "Token-based pagination: returns the next token (from body or headers)."
  [{:keys [next-token]}]
  (when (seq next-token)
    {:page_token next-token}))

;; --- Time-based Pagination ---
(defn time-next
  "Time-based pagination: uses updated_at or timestamp to get next window."
  [{:keys [last-updated window-size]}]
  (when last-updated
    {:updated_since last-updated
     :window-size window-size}))

(defn parse-link-header [header]
  (let [cfg (config/load-config)
        regex-str (:link-header-regex cfg)]
  (log/info "Raw Link header input:" header)
  (if (str/blank? header)
    {}
    (->> (str/split header #",")
         (map str/trim)
         (map #(re-find (re-pattern regex-str) %)) ;; FIXED: re-find instead of re-matches
         (keep identity)
         (reduce (fn [acc [_ url rel]]
                   (assoc acc rel url)) ;; rel as string so "next", not :next
                 {})))))

(defn extract-page [url]
  (let [cfg (config/load-config)
        pattern-str (or (:page-param-regex cfg) "[&?]page=(\\d+)")
        pattern (re-pattern pattern-str)
        match (when url (re-find pattern url))]
    (when-let [[_ s] match]
      (parse-long s))))

(defn link-header-next
  "Parses Link header, logs current/last page, and returns next URL."
  [{:keys [response]}]
  (let [link-h   (get-in response [:headers "link"])
        _ (log/info "Link header: " link-h)
        links    (parse-link-header link-h)
        _ (log/info "Links : " links)
        url-next (get links "next")
        _ (log/info "url-next : " url-next)
        url-prev (get links "prev")
        url-last (get links "last")
        page-next (some-> url-next extract-page)
        _ (log/info "page-next : " page-next)
        page-prev (some-> url-prev extract-page)
        page-last (some-> url-last extract-page)
        _ (log/info "page-last : " page-last)
        page-cur (cond
                   page-next (dec page-next)
                   page-prev (inc page-prev)
                   :else 1)
        _ (log/info "page-cur : " page-cur)
        ]
    (when (pos? page-cur)
      (if page-last
        (log/infof "GitHub pagination: Page %d of %d" page-cur page-last)
        (log/infof "GitHub pagination: Page %d (total unknown)" page-cur)))
    (when url-last
      (log/debugf "Last page URL: %s" url-last))
    (if url-next
      {:next-url url-next}
      (do
        (log/info "Reached last page — no more pagination.")
        nil))))


;; --- Dispatcher ---
(defmulti next-page :type)

(defmethod next-page :offset [state] (offset-next state))
(defmethod next-page :page [state] (page-next state))
(defmethod next-page :cursor [state] (cursor-next state))
(defmethod next-page :token [state] (token-next state))
(defmethod next-page :time [state] (time-next state))
(defmethod next-page :link-header [state] (link-header-next state))
(defmethod next-page :default [_]
  (log/warn "Unknown pagination type") nil)

