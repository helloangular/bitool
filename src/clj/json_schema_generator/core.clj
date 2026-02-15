;; JSON Schema Generator for UI Tree Display
(ns json-schema-generator.core
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clj-http.client :as http]
            [clojure.walk :as walk]
            [clojure.java.io :as io]))

;; JSON Schema Generator for UI Tree Display
(ns json-schema-generator.core
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [clj-http.client :as http]
            [clojure.walk :as walk]
            [clojure.java.io :as io]))

;; =============================================================================
;; CORE SCHEMA RESOLUTION FUNCTIONS (REQUIRED DEPENDENCIES)
;; =============================================================================

(defn resolve-ref
  "Resolve a $ref reference to actual schema definition"
  [ref-path openapi-spec]
  (when ref-path
    (let [clean-path (str/replace ref-path #"^#/" "")
          path-parts (map keyword (str/split clean-path #"/"))
          resolved (get-in openapi-spec path-parts)]
      resolved)))

(defn resolve-all-refs
  "Recursively resolve all $ref references in a schema"
  [schema openapi-spec]
  (walk/postwalk
    (fn [node]
      (if (and (map? node) (:$ref node))
        (let [resolved (resolve-ref (:$ref node) openapi-spec)]
          (if resolved
            (resolve-all-refs resolved openapi-spec)
            node))
        node))
    schema))

(defn get-complete-schema
  "Get complete schema with all $ref references resolved"
  [spec endpoint method]
  (let [base-schema (-> spec
                       :paths
                       (get (keyword endpoint))
                       (get (keyword method))
                       :responses
                       (get (keyword "200"))
                       :content
                       (get (keyword "application/json"))
                       :schema)]
    (when base-schema
      (resolve-all-refs base-schema spec))))

(defn generate-json-schema-for-ui
  "Generate JSON schema suitable for JavaScript tree components"
  [complete-schema endpoint-name]
  (let [base-schema (assoc complete-schema
                          :$schema "https://json-schema.org/draft/2020-12/schema"
                          :$id (str "https://api.github.com/schemas/" endpoint-name)
                          :title (str "GitHub " (str/capitalize endpoint-name) " Schema")
                          :description (str "Complete schema for GitHub " endpoint-name " endpoint"))]
    
    ;; Ensure all nested objects have proper metadata for tree display
    (walk/postwalk
      (fn [node]
        (if (and (map? node) (= (:type node) "object"))
          (assoc node 
                 :additionalProperties false
                 :$comment "Resolved from GitHub OpenAPI specification")
          node))
      base-schema)))

(defn save-json-schema
  "Save JSON schema to file for UI consumption"
  [schema filename]
  (try
    (spit filename (json/write-str schema :indent true :key-fn name))
    (println "✅ JSON schema saved to:" filename)
    {:success true :file filename}
    (catch Exception e
      (println "❌ Error saving schema:" (.getMessage e))
      {:success false :error (.getMessage e)})))

(defn generate-github-issues-json-schema
  "Generate complete GitHub issues JSON schema"
  []
  (println "🔄 Generating GitHub Issues JSON Schema...")
  
  ;; Get complete schema (using previous functions)
  (let [github-spec (-> (http/get "https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json" {:as :json})
                       :body)
        complete-schema (get-complete-schema github-spec "/repos/{owner}/{repo}/issues" "get")
        
        ;; Generate UI-friendly JSON schema
        json-schema (generate-json-schema-for-ui complete-schema "issues")]
    
    (if json-schema
      (do
        (println "✅ JSON schema generated successfully!")
        (println "Schema type:" (:type json-schema))
        (println "Items type:" (get-in json-schema [:items :type]))
        (println "Total properties:" (count (get-in json-schema [:items :properties])))
        
        ;; Print the JSON schema
        (println "\n📋 JSON Schema Output:")
        (println (json/write-str json-schema {:indent true :key-fn name}))
        
        json-schema)
      (println "❌ Failed to generate schema"))))

(defn generate-all-github-schemas
  "Generate JSON schemas for common GitHub endpoints"
  []
  (let [endpoints {"issues" "/repos/{owner}/{repo}/issues"
                   "pulls" "/repos/{owner}/{repo}/pulls"
                   "repos" "/user/repos"
                   "commits" "/repos/{owner}/{repo}/commits"
                   "releases" "/repos/{owner}/{repo}/releases"
                   "events" "/users/{username}/events"}
        
        github-spec (-> (http/get "https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json" {:as :json})
                       :body)
        
        results (into {} 
                     (map (fn [[name endpoint]]
                            (println "Generating schema for" name "...")
                            (let [complete-schema (get-complete-schema github-spec endpoint "get")
                                  json-schema (generate-json-schema-for-ui complete-schema name)]
                              [name json-schema]))
                          endpoints))]
    
    (println "\n📊 Generated schemas summary:")
    (doseq [[name schema] results]
      (let [prop-count (if (= (:type schema) "array")
                        (count (get-in schema [:items :properties]))
                        (count (:properties schema)))]
        (println " " name ":" prop-count "properties")))
    
    results))

;; =============================================================================
;; SCHEMA ENHANCEMENT FOR UI DISPLAY
;; =============================================================================

(defn add-ui-metadata
  "Add metadata useful for UI tree components"
  [schema field-path]
  (walk/postwalk
    (fn [node]
      (if (map? node)
        (let [enhanced-node (cond-> node
                             ;; Add display names
                             (:type node) (assoc :displayName 
                                                 (str/join " > " field-path))
                             
                             ;; Add icons based on type
                             (= (:type node) "string") (assoc :icon "text")
                             (= (:type node) "integer") (assoc :icon "number") 
                             (= (:type node) "boolean") (assoc :icon "boolean")
                             (= (:type node) "array") (assoc :icon "list")
                             (= (:type node) "object") (assoc :icon "object")
                             
                             ;; Add format-specific icons
                             (= (:format node) "date-time") (assoc :icon "calendar")
                             (= (:format node) "uri") (assoc :icon "link")
                             (= (:format node) "email") (assoc :icon "email")
                             
                             ;; Mark required fields
                             true (assoc :isRequired false))]
          enhanced-node)
        node))
    schema))

(defn generate-ui-tree-schema
  "Generate schema optimized for JavaScript tree components"
  [endpoint-name]
  (println "🎨 Generating UI-optimized schema for" endpoint-name)
  
  (let [github-spec (-> (http/get "https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json" {:as :json})
                       :body)
        endpoint-path (case endpoint-name
                       "issues" "/repos/{owner}/{repo}/issues"
                       "pulls" "/repos/{owner}/{repo}/pulls"
                       "repos" "/user/repos"
                       "commits" "/repos/{owner}/{repo}/commits"
                       "releases" "/repos/{owner}/{repo}/releases"
                       "events" "/users/{username}/events")
        
        complete-schema (get-complete-schema github-spec endpoint-path "get")
        ui-schema (-> complete-schema
                     (generate-json-schema-for-ui endpoint-name)
                     (add-ui-metadata [endpoint-name]))]
    
    ui-schema))

;; =============================================================================
;; READY-TO-USE FUNCTIONS
;; =============================================================================

(defn get-github-issues-json-schema
  "Get complete GitHub issues schema as JSON string"
  []
  (let [schema (generate-ui-tree-schema "issues")]
    (json/write-str schema {:indent true :key-fn name})))

(defn save-github-issues-schema-to-file
  "Save GitHub issues schema to JSON file"
  [filename]
  (let [schema (generate-ui-tree-schema "issues")]
    (save-json-schema schema filename)))

(defn print-json-schema
  "Print JSON schema to console (copy-paste ready)"
  [endpoint-name]
  (let [schema (generate-ui-tree-schema endpoint-name)
        json-str (json/write-str schema :indent true :key-fn name)]
    (println "🎯" (str/upper-case endpoint-name) "JSON SCHEMA:")
    (println "=" (apply str (repeat 60 "=")))
    (println json-str)
    json-str))

(defn generate-schema-files
  "Generate JSON schema files for all endpoints"
  [output-dir]
  (.mkdirs (java.io.File. output-dir))
  
  (let [endpoints ["issues" "pulls" "repos" "commits" "releases" "events"]]
    (doseq [endpoint endpoints]
      (let [schema (generate-ui-tree-schema endpoint)
            filename (str output-dir "/" endpoint "-schema.json")]
        (save-json-schema schema filename)))
    
    (println "\n✅ All schema files generated in:" output-dir)))

;; =============================================================================
;; SAMPLE OUTPUT PREVIEW
;; =============================================================================

(defn show-schema-preview
  "Show a preview of what the JSON schema looks like"
  []
  (println "📋 Sample JSON Schema Structure:")
  (println "{")
  (println "  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",")
  (println "  \"$id\": \"https://api.github.com/schemas/issues\",")
  (println "  \"title\": \"GitHub Issues Schema\",")
  (println "  \"type\": \"array\",")
  (println "  \"items\": {")
  (println "    \"type\": \"object\",")
  (println "    \"properties\": {")
  (println "      \"id\": {\"type\": \"integer\", \"icon\": \"number\"},")
  (println "      \"title\": {\"type\": \"string\", \"icon\": \"text\"},")
  (println "      \"body\": {\"type\": \"string\", \"icon\": \"text\"},")
  (println "      \"state\": {\"type\": \"string\", \"enum\": [\"open\", \"closed\"]},")
  (println "      \"created_at\": {\"type\": \"string\", \"format\": \"date-time\", \"icon\": \"calendar\"},")
  (println "      \"user\": {")
  (println "        \"type\": \"object\",")
  (println "        \"icon\": \"object\",") 
  (println "        \"properties\": {")
  (println "          \"id\": {\"type\": \"integer\", \"icon\": \"number\"},")
  (println "          \"login\": {\"type\": \"string\", \"icon\": \"text\"},")
  (println "          \"avatar_url\": {\"type\": \"string\", \"format\": \"uri\", \"icon\": \"link\"}")
  (println "          // ... more user fields")
  (println "        }")
  (println "      },")
  (println "      \"labels\": {")
  (println "        \"type\": \"array\",")
  (println "        \"icon\": \"list\",")
  (println "        \"items\": {")
  (println "          \"type\": \"object\",")
  (println "          \"properties\": {")
  (println "            \"name\": {\"type\": \"string\", \"icon\": \"text\"},")
  (println "            \"color\": {\"type\": \"string\", \"icon\": \"text\"}")
  (println "            // ... more label fields")
  (println "          }")
  (println "        }")
  (println "      }")
  (println "      // ... 50+ more fields")
  (println "    }")
  (println "  }")
  (println "}"))

;; =============================================================================
;; USAGE INSTRUCTIONS
;; =============================================================================

(println "📝 JSON Schema Generator Ready!")
(println "\nQuick usage:")
(println "  (print-json-schema \"issues\")              ; Print issues schema")
(println "  (get-github-issues-json-schema)            ; Get as JSON string")
(println "  (save-github-issues-schema-to-file \"issues.json\") ; Save to file")
(println "  (generate-schema-files \"./schemas\")       ; Generate all schemas")
(println "  (show-schema-preview)                      ; See sample structure")
(println "\nThe output will be valid JSON Schema that works with:")
(println "  - React JSON Schema Form")
(println "  - JSON Schema Tree components")
(println "  - Any JSON Schema validator")
(println "  - Documentation generators")

;; Quick demo function
(defn quick-demo []
  (println "🚀 Quick JSON Schema Demo\n")
  (print-json-schema "issues"))

(println "\nRun (quick-demo) to see the complete GitHub Issues JSON Schema! 🎉")