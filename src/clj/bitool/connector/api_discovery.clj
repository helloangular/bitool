(ns bitool.connector.api-discovery
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]))

(defn fetch-json [url]
  (try
    (let [response (http/get url {:as :json})]
      (:body response))
    (catch Exception e
      (log/error e (str "Failed to fetch URL: " url))
      nil)))

(defn fetch-text [url]
  (try
    (let [response (http/get url {:as :text})]
      (:body response))
    (catch Exception e
      (log/error e (str "Failed to fetch URL: " url))
      nil)))

(defn parse-html-links [html]
  (->> (re-seq #"href=\"(\/[^\"]+)\"" html)
       (map second)
       (distinct)))

(defn discover-rest-endpoints [doc-url link-pattern]
  (let [docs-html (fetch-text doc-url)
        links (->> (parse-html-links docs-html)
                   (filter #(re-find link-pattern %))
                   (map #(str (if (.startsWith % "/") "https://" (subs doc-url 0 (.indexOf doc-url "/en"))) %))
                   set)]
    links))

(defn discover-graphql-schema [graphql-url token-env-var]
  (let [token (System/getenv token-env-var)
        introspection-query {:query "query { __schema { types { name kind fields { name } } } }"}
        headers {"Authorization" (str "bearer " token)}
        response (http/post graphql-url {:headers headers
                                         :body (json/generate-string introspection-query)
                                         :content-type :json
                                         :as :json})]
    (get-in response [:body :data :__schema :types])))

(defn discover-endpoints [base-url]
  (let [result
        (cond
          (re-find #"api\.github\.com" base-url)
          {:rest (discover-rest-endpoints "https://docs.github.com/en/rest" #"^/en/rest/.+/.+")
           :graphql (discover-graphql-schema "https://api.github.com/graphql" "GITHUB_TOKEN")}

          :else
          (do
            (log/warn "Discovery not implemented for base URL:" base-url)
            nil))]
    (when result
      (println (with-out-str (clojure.pprint/pprint result))))
    result))

