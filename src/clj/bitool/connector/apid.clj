(ns bitool.connector.apid
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [clojure.walk :as walk]))

;; Configuration
(def default-timeout 10000)

;; Known OpenAPI spec locations for popular APIs
(def known-openapi-specs
  {"api.github.com" 
   {:spec-url "https://raw.githubusercontent.com/github/rest-api-description/refs/heads/main/descriptions-next/api.github.com/api.github.com.json"
    :description "GitHub REST API v3/v4 - Complete OpenAPI specification"
    :note "Official GitHub OpenAPI spec with 200+ endpoints"}
   
   "petstore.swagger.io"
   {:spec-url "https://petstore.swagger.io/v2/swagger.json"
    :description "Swagger Petstore - Demo API"
    :note "Classic Swagger example API"}
   
   "httpbin.org"
   {:spec-url "https://httpbin.org/spec.json"
    :description "HTTPBin - HTTP testing service"
    :note "Simple HTTP testing endpoints"}
   
   "jsonplaceholder.typicode.com"
   {:spec-url "https://jsonplaceholder.typicode.com/openapi.json"
    :description "JSONPlaceholder - Fake REST API"
    :note "Demo REST API for testing (may not exist)"
    :fallback true}})

;; Common OpenAPI spec paths for auto-discovery
(def common-spec-paths
  ["/swagger.json" "/openapi.json" "/api-docs" "/api-docs.json"
   "/swagger/v1/swagger.json" "/v1/swagger.json" "/api/swagger.json"
   "/docs/swagger.json" "/spec.json" "/.well-known/openapi"])

(defn make-url
  "Safely join base URL with path"
  [base-url path]
  (let [base (str/replace base-url #"/$" "")
        path (if (str/starts-with? path "/") path (str "/" path))]
    (str base path)))

(defn safe-request
  "Make HTTP request with error handling"
  [method url & [options]]
  (try
    (http/request (merge {:method method
                          :url url
                          :socket-timeout default-timeout
                          :connection-timeout default-timeout
                          :throw-exceptions false
                          :accept :json}
                         options))
    (catch Exception e
      {:error (.getMessage e) :url url})))

(defn parse-json-content
  "Parse JSON response content"
  [response]
  (try
    (when-let [body (:body response)]
      (json/parse-string body true))
    (catch Exception e
      (println (str "⚠️  Failed to parse JSON: " (.getMessage e)))
      nil)))

(defn validate-openapi-spec
  "Validate that content is a valid OpenAPI/Swagger spec"
  [spec]
  (or (:openapi spec)    ; OpenAPI 3.x
      (:swagger spec)    ; Swagger 2.0
      (:paths spec)))    ; Generic spec with paths

(defn fetch-openapi-spec
  "Fetch and validate OpenAPI specification from URL"
  [spec-url headers]
  (println (str "📥 Fetching OpenAPI spec from: " spec-url))
  (let [response (safe-request :get spec-url {:headers headers})]
    (cond
      (:error response)
      (do
        (println (str "❌ Error fetching spec: " (:error response)))
        nil)
      
      (not= 200 (:status response))
      (do
        (println (str "❌ HTTP " (:status response) " - Could not fetch spec"))
        nil)
      
      :else
      (let [spec (parse-json-content response)]
        (if (validate-openapi-spec spec)
          (do
            (println "✅ Valid OpenAPI specification found!")
            spec)
          (do
            (println "❌ Invalid or unrecognized API specification format")
            nil))))))

(defn get-host-from-url
  "Extract hostname from URL"
  [url]
  (-> url
      (str/replace #"https?://" "")
      (str/split #"/")
      first))

(defn suggest-known-specs
  "Suggest known OpenAPI specs for the given API"
  [base-url]
  (let [host (get-host-from-url base-url)
        known-spec (get known-openapi-specs host)]
    (when known-spec
      (println (str "\n💡 Found known OpenAPI spec for " host ":"))
      (println (str "   📄 " (:description known-spec)))
      (println (str "   🔗 " (:spec-url known-spec)))
      (when (:note known-spec)
        (println (str "   ℹ️  " (:note known-spec))))
      known-spec)))

(defn prompt-for-spec-url
  "Interactive prompt for OpenAPI spec URL"
  [base-url]
  (println "\n❓ No OpenAPI specification found automatically.")
  (println "📝 You can provide an OpenAPI/Swagger spec URL for comprehensive discovery.")
  (println "\nOptions:")
  (println "1. Enter a direct OpenAPI spec URL")
  (println "2. Check the API's documentation for OpenAPI/Swagger links")
  (println "3. Search GitHub for '[API-name] openapi' repositories")
  (println "4. Try the API developer portal or docs site")
  (println "\nCommon spec URL patterns:")
  (println "• https://api.example.com/swagger.json")
  (println "• https://api.example.com/openapi.json") 
  (println "• https://docs.example.com/api-spec.json")
  (println "• https://raw.githubusercontent.com/company/api-specs/main/openapi.json")
  
  ;; Suggest known spec if available
  (if-let [known-spec (suggest-known-specs base-url)]
    (do
      (println (str "\n✨ Use known spec? (y/n): " (:spec-url known-spec)))
      (print ">>> ")
      (flush)
      (let [choice (str/trim (read-line))]
        (if (contains? #{"y" "yes" "Y" "YES"} choice)
          (:spec-url known-spec)
          ;; If they said no to known spec, still ask for manual input
          (do
            (println "\n🔗 Enter OpenAPI spec URL (or press Enter to skip):")
            (print ">>> ")
            (flush)
            (let [input (str/trim (read-line))]
              (when (and (seq input) (str/starts-with? input "http"))
                input))))))
    ;; No known spec, just ask for manual input
    (do
      (println "\n🔗 Enter OpenAPI spec URL (or press Enter to skip):")
      (print ">>> ")
      (flush)
      (let [input (str/trim (read-line))]
        (when (and (seq input) (str/starts-with? input "http"))
          input)))))

(defn auto-discover-spec
  "Attempt automatic OpenAPI spec discovery"
  [base-url headers]
  (println "🔍 Searching for OpenAPI specification...")
  (loop [paths common-spec-paths]
    (when (seq paths)
      (let [path (first paths)
            url (make-url base-url path)
            response (safe-request :get url {:headers headers})]
        (if (and (not (:error response))
                 (= 200 (:status response)))
          (let [spec (parse-json-content response)]
            (if (validate-openapi-spec spec)
              (do
                (println (str "✅ Found OpenAPI spec at: " url))
                {:url url :spec spec})
              (recur (rest paths))))
          (recur (rest paths)))))))

(defn parse-openapi-v3
  "Parse OpenAPI 3.x specification"
  [spec base-url]
  (let [paths (:paths spec)
        servers (:servers spec)
        base-server (or (-> servers first :url) base-url)]
    (for [[path methods] paths
          [method details] methods
          :when (keyword? method)]
      {:method (str/upper-case (name method))
       :path path
       :url (make-url base-server path)
       :summary (:summary details "")
       :description (:description details "")
       :operation-id (:operationId details)
       :tags (:tags details [])
       :parameters (map #(select-keys % [:name :in :required :description :schema]) 
                        (:parameters details []))
       :request-body (:requestBody details)
       :responses (:responses details)
       :security (:security details)
       :deprecated (:deprecated details false)
       :spec-version "3.x"})))

(defn parse-swagger-v2
  "Parse Swagger 2.0 specification" 
  [spec base-url]
  (let [paths (:paths spec)
        host (:host spec)
        base-path (:basePath spec "")
        schemes (:schemes spec ["https"])
        full-base (if host 
                    (str (first schemes) "://" host base-path)
                    base-url)]
    (for [[path methods] paths
          [method details] methods
          :when (keyword? method)]
      {:method (str/upper-case (name method))
       :path path
       :url (make-url full-base path)
       :summary (:summary details "")
       :description (:description details "")
       :operation-id (:operationId details)
       :tags (:tags details [])
       :parameters (map #(select-keys % [:name :in :required :description :type :format])
                        (:parameters details []))
       :consumes (:consumes details)
       :produces (:produces details)
       :responses (:responses details)
       :security (:security details)
       :deprecated (:deprecated details false)
       :spec-version "2.0"})))

(defn extract-endpoints-from-spec
  "Extract endpoints from OpenAPI specification"
  [spec base-url]
  (cond
    ;; OpenAPI 3.x
    (:openapi spec)
    (parse-openapi-v3 spec base-url)
    
    ;; Swagger 2.0
    (:swagger spec)
    (parse-swagger-v2 spec base-url)
    
    ;; Generic spec with paths
    (:paths spec)
    (parse-openapi-v3 spec base-url) ; Try OpenAPI 3.x parser as fallback
    
    :else
    []))

(defn analyze-endpoint-coverage
  "Analyze the comprehensiveness of discovered endpoints"
  [endpoints]
  (let [methods (frequencies (map :method endpoints))
        paths-by-tag (group-by :tags endpoints)
        auth-required (count (filter :security endpoints))
        deprecated (count (filter :deprecated endpoints))
        unique-paths (count (distinct (map :path endpoints)))]
    
    (println "\n📊 Endpoint Analysis:")
    (println (str "📈 Total endpoints: " (count endpoints)))
    (println (str "🛣️  Unique paths: " unique-paths))
    (println (str "🔧 HTTP methods: " (str/join ", " (keys methods))))
    (println (str "📋 Methods distribution: " methods))
    (println (str "🔒 Endpoints requiring auth: " auth-required))
    (println (str "⚠️  Deprecated endpoints: " deprecated))
    (when (seq (remove empty? (keys paths-by-tag)))
      (println (str "🏷️  Tags: " (str/join ", " (remove empty? (keys paths-by-tag))))))
    
    {:total (count endpoints)
     :unique-paths unique-paths
     :methods methods
     :auth-required auth-required
     :deprecated deprecated
     :tags (keys paths-by-tag)}))

(defn format-endpoint-details
  "Format detailed endpoint information"
  [endpoint]
  (println (str "🔸 " (:method endpoint) " " (:path endpoint)))
  (println (str "   URL: " (:url endpoint)))
  
  (when-let [summary (:summary endpoint)]
    (when (not (str/blank? summary))
      (println (str "   📄 " summary))))
  
  (when-let [description (:description endpoint)]
    (when (not (str/blank? description))
      (println (str "   📝 " description))))
  
  (when-let [op-id (:operation-id endpoint)]
    (println (str "   🆔 Operation: " op-id)))
  
  (when-let [tags (:tags endpoint)]
    (when (seq tags)
      (println (str "   🏷️  Tags: " (str/join ", " tags)))))
  
  (when-let [params (:parameters endpoint)]
    (when (seq params)
      (println "   📋 Parameters:")
      (doseq [param params]
        (println (str "      • " (:name param) " (" (:in param) ")"
                      (when (:required param) " [required]")
                      (when (:description param) (str " - " (:description param))))))))
  
  (when (:deprecated endpoint)
    (println "   ⚠️  DEPRECATED"))
  
  (when (:security endpoint)
    (println "   🔒 Requires authentication"))
  
  (println))

(defn discover-endpoints
  "Main interactive discovery function"
  ([base-url-or-spec] (discover-endpoints base-url-or-spec {}))
  ([base-url-or-spec headers]
   (cond
     ;; Direct OpenAPI spec URL provided
     (and (string? base-url-or-spec) 
          (or (str/includes? base-url-or-spec "swagger")
              (str/includes? base-url-or-spec "openapi")
              (str/includes? base-url-or-spec ".json")
              (str/includes? base-url-or-spec ".yaml")))
     (do
       (println "🎯 Direct OpenAPI spec URL detected")
       (if-let [spec (fetch-openapi-spec base-url-or-spec headers)]
         (let [endpoints (extract-endpoints-from-spec spec base-url-or-spec)]
           (println (str "\n📋 Discovered " (count endpoints) " endpoints from specification"))
           (analyze-endpoint-coverage endpoints)
           (println "\n📋 Endpoints:")
           (println (str/join "" (repeat 60 "=")))
           (doseq [endpoint endpoints]
             (format-endpoint-details endpoint))
           endpoints)
         []))
     
     ;; Regular API base URL
     :else
     (let [base-url base-url-or-spec]
       (println (str "🚀 Starting API discovery for: " base-url "\n"))
       
       ;; Try automatic discovery first
       (if-let [auto-spec (auto-discover-spec base-url headers)]
         ;; Found spec automatically
         (let [endpoints (extract-endpoints-from-spec (:spec auto-spec) base-url)]
           (println (str "\n📋 Discovered " (count endpoints) " endpoints from automatic discovery"))
           (analyze-endpoint-coverage endpoints)
           (println "\n📋 Endpoints:")
           (println (str/join "" (repeat 60 "=")))
           (doseq [endpoint endpoints]
             (format-endpoint-details endpoint))
           endpoints)
         
         ;; No automatic discovery - prompt user
         (if-let [user-spec-url (prompt-for-spec-url base-url)]
           ;; User provided spec URL
           (if-let [spec (fetch-openapi-spec user-spec-url headers)]
             (let [endpoints (extract-endpoints-from-spec spec base-url)]
               (println (str "\n📋 Discovered " (count endpoints) " endpoints from user-provided specification"))
               (analyze-endpoint-coverage endpoints)
               (println "\n📋 Endpoints:")
               (println (str/join "" (repeat 60 "=")))
               (doseq [endpoint endpoints]
                 (format-endpoint-details endpoint))
               endpoints)
             [])
           
           ;; User skipped - provide manual guidance
           (do
             (println "\n❌ No OpenAPI specification available")
             (println "\n💡 Manual discovery suggestions:")
             (println "   1. Check the API's official documentation")
             (println "   2. Look for 'OpenAPI', 'Swagger', or 'API Schema' links")
             (println "   3. Search GitHub: '[API-name] openapi specification'")
             (println "   4. Check developer portals or API reference pages")
             (println "   5. Try browser DevTools to inspect API calls")
             (println "\n🔗 Common spec locations to try:")
             (doseq [path common-spec-paths]
               (println (str "   • " (make-url base-url path))))
             [])))))))

;; Convenience functions
(defn discover-with-spec
  "Discover endpoints using a specific OpenAPI spec URL"
  [spec-url & [headers]]
  (discover-endpoints spec-url (or headers {})))

(defn discover-github-api
  "Quick discovery for GitHub API using known spec"
  []
  (discover-with-spec "https://raw.githubusercontent.com/github/rest-api-description/refs/heads/main/descriptions-next/api.github.com/api.github.com.json"))

;; Output formatting utilities
(defn list-endpoint-urls
  "Output just the HTTP method and path for each endpoint"
  [endpoints]
  (println "\n📋 Endpoint URLs:")
  (println (str/join "" (repeat 50 "=")))
  (doseq [endpoint (sort-by #(str (:method %) " " (:path %)) endpoints)]
    (println (str (:method endpoint) " " (:path endpoint))))
  
  ;; Return nil to avoid printing the list
  nil)

(defn list-endpoint-urls-with-return
  "Output endpoint URLs and return the list for programmatic use"
  [endpoints]
  (println "\n📋 Endpoint URLs:")
  (println (str/join "" (repeat 50 "=")))
  (let [url-list (map #(str (:method %) " " (:path %)) endpoints)]
    (doseq [url (sort url-list)]
      (println url))
    url-list))

(defn list-endpoint-urls-grouped
  "Output endpoint URLs grouped by HTTP method"
  [endpoints]
  (let [grouped (group-by :method endpoints)]
    (println "\n📋 Endpoint URLs (Grouped by Method):")
    (println (str/join "" (repeat 50 "=")))
    (doseq [[method eps] (sort grouped)]
      (println (str "\n" method ":"))
      (doseq [endpoint (sort-by :path eps)]
        (println (str "  " (:path endpoint)))))
    
    ;; Return nil to avoid printing the grouped structure
    nil))

(defn list-endpoint-urls-with-tags
  "Output endpoint URLs grouped by tags"
  [endpoints]
  (let [tagged-endpoints (group-by #(if (seq (:tags %)) 
                                      (first (:tags %)) 
                                      "untagged") 
                                   endpoints)]
    (println "\n📋 Endpoint URLs (Grouped by Tags):")
    (println (str/join "" (repeat 50 "=")))
    (doseq [[tag eps] (sort tagged-endpoints)]
      (println (str "\n🏷️  " tag ":"))
      (doseq [endpoint (sort-by #(str (:method %) " " (:path %)) eps)]
        (println (str "  " (:method endpoint) " " (:path endpoint)))))
    
    ;; Return nil to avoid printing the tagged structure
    nil))

(defn get-endpoint-urls-list
  "Get endpoint URLs as a list without printing"
  [endpoints]
  (map #(str (:method %) " " (:path %)) endpoints))

(defn export-endpoint-urls
  "Export just the endpoint URLs to a file"
  [endpoints filename & [format]]
  (let [url-list (map #(str (:method %) " " (:path %)) endpoints)]
    (case (or format :txt)
      :txt (spit filename (str/join "\n" (sort url-list)))
      :json (spit filename (json/generate-string (sort url-list) {:pretty true}))
      :edn (spit filename (pr-str (sort url-list))))
    (println (str "📝 Endpoint URLs exported to " filename))
    (count url-list)))
(defn save-endpoints-as-json
  "Save discovered endpoints as JSON"
  [endpoints filename]
  (spit filename (json/generate-string endpoints {:pretty true}))
  (println (str "💾 Endpoints saved to " filename)))

(defn export-to-postman
  "Export endpoints to Postman collection format"
  [endpoints collection-name]
  {:info {:name collection-name
          :description "Generated from OpenAPI specification"}
   :item (map (fn [ep]
                {:name (or (:summary ep) (str (:method ep) " " (:path ep)))
                 :request {:method (:method ep)
                           :header []
                           :url {:raw (:url ep)
                                 :protocol "https"
                                 :host (str/split (get-host-from-url (:url ep)) #"\.")
                                 :path (filter seq (str/split (:path ep) #"/"))}}})
              endpoints)})

(defn save-as-postman
  "Save endpoints as Postman collection"
  [endpoints filename collection-name]
  (let [collection (export-to-postman endpoints collection-name)]
    (spit filename (json/generate-string collection {:pretty true}))
    (println (str "📮 Postman collection saved to " filename))))
;; Convenience functions with URL listing
(defn discover-and-list-urls
  "Discover endpoints and immediately show just the URL list"
  ([base-url-or-spec] (discover-and-list-urls base-url-or-spec {}))
  ([base-url-or-spec headers]
   (let [endpoints (discover-endpoints base-url-or-spec headers)]
     (when (seq endpoints)
       (list-endpoint-urls endpoints))
     endpoints)))

(defn discover-and-list-urls-grouped
  "Discover endpoints and show URLs grouped by method"
  ([base-url-or-spec] (discover-and-list-urls-grouped base-url-or-spec {}))
  ([base-url-or-spec headers]
   (let [endpoints (discover-endpoints base-url-or-spec headers)]
     (when (seq endpoints)
       (list-endpoint-urls-grouped endpoints))
     endpoints)))

(defn quick-github-urls
  "Quick function to get just GitHub API endpoint URLs"
  []
  (let [endpoints (discover-github-api)]
    (when (seq endpoints)
      (list-endpoint-urls endpoints))
    endpoints))

(defn discover-with-spec
  "Discover endpoints using a specific OpenAPI spec URL"
  [spec-url & [headers]]
  (discover-endpoints spec-url (or headers {})))

(defn list-known-apis
  "List all known APIs with OpenAPI specs"
  []
  (println "🗂️  Known APIs with OpenAPI specifications:")
  (doseq [[host info] known-openapi-specs]
    (println (str "\n🔸 " host))
    (println (str "   📄 " (:description info)))
    (println (str "   🔗 " (:spec-url info)))
    (when (:note info)
      (println (str "   ℹ️  " (:note info))))))


;; Example usage and help
(defn examples []
  (println "🚀 API Discovery Examples:")
  (println "\n1. Auto-discovery (tries to find OpenAPI spec automatically):")
  (println "   (discover-endpoints \"https://api.github.com\")")
  (println "\n2. Direct OpenAPI spec URL:")
  (println "   (discover-with-spec \"https://raw.githubusercontent.com/github/rest-api-description/refs/heads/main/descriptions-next/api.github.com/api.github.com.json\")")
  (println "\n3. Quick GitHub API discovery:")
  (println "   (discover-github-api)")
  (println "\n4. Just list endpoint URLs:")
  (println "   (discover-and-list-urls \"https://api.github.com\")")
  (println "   (quick-github-urls)")
  (println "\n5. List URLs grouped by method:")
  (println "   (discover-and-list-urls-grouped \"https://api.github.com\")")
  (println "\n6. Export just the URLs:")
  (println "   (let [eps (discover-endpoints \"https://api.github.com\")]")
  (println "     (export-endpoint-urls eps \"endpoints.txt\"))")
  (println "     (list-endpoint-urls eps)")
  (println "\n7. List known APIs:")
  (println "   (list-known-apis)")
  (println "\n8. With authentication:")
  (println "   (discover-endpoints \"https://api.example.com\" {\"Authorization\" \"Bearer token\"})")
  (println "\n9. Save full results:")
  (println "   (let [eps (discover-endpoints \"https://api.example.com\")]")
  (println "     (save-as-postman eps \"api.json\" \"My API Collection\"))")
  
  (println "\n💡 The tool will prompt you for OpenAPI spec URLs when auto-discovery fails!")
  (println "\n📋 URL Listing Functions:")
  (println "   • (list-endpoint-urls endpoints) - Simple list")
  (println "   • (list-endpoint-urls-grouped endpoints) - Grouped by HTTP method") 
  (println "   • (list-endpoint-urls-with-tags endpoints) - Grouped by tags")
  (println "   • (export-endpoint-urls endpoints \"file.txt\") - Export to file"))

;; Main entry point
(defn -main [& args]
  (if (seq args)
    (discover-endpoints (first args))
    (examples)))
