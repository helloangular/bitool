(ns bitool.connector.schema
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]))

;; Atom to hold loaded OpenAPI spec
(def github-api-spec (atom nil))

;; Load the GitHub OpenAPI spec JSON file from local disk
(defn load-spec-from-file! [file]
  (reset! github-api-spec
          (-> file slurp (json/parse-string true)))
  (println "✅ GitHub OpenAPI spec loaded from file."))

(defn load-spec-from-url!
  "Loads the GitHub OpenAPI spec JSON from a URL and stores it in the github-api-spec atom."
  [url]
  (try
    (println "🔄 Loading OpenAPI spec from URL:" url)
    (let [response (slurp url)
          parsed (json/parse-string response true)]
      (reset! github-api-spec parsed)
      (println "✅ Loaded OpenAPI spec from:" url)
      (println "📊 Spec info:")
      (println "  - OpenAPI version:" (:openapi parsed))
      (println "  - Title:" (get-in parsed [:info :title]))
      (println "  - Version:" (get-in parsed [:info :version]))
      (println "  - Total paths:" (count (:paths parsed)))
      (println "  - Schema components:" (count (get-in parsed [:components :schemas])))
      true)
    (catch Exception e
      (println "❌ Failed to load spec from URL:" (.getMessage e))
      false)))

(defn load-spec!
  "Loads spec from file or URL. Detects URL if starts with http."
  [source]
  (if (str/starts-with? source "http")
    (load-spec-from-url! source)
    (load-spec-from-file! source)))

;; Check if a path exists in the spec
(defn path-exists? [path]
  (let [path-key (if (string? path) (keyword path) path)
        exists? (contains? (:paths @github-api-spec) path-key)]
    (println (if exists?
               (str "✅ Path exists: " path)
               (str "❌ Path does NOT exist: " path)))
    exists?))

;; List all matching paths that contain a substring (e.g., "issues")
(defn list-matching-paths [substring]
  (let [matching-paths (->> (keys (:paths @github-api-spec))
                           (map name)  ; Convert keywords to strings for searching
                           (filter #(str/includes? % substring))
                           (sort))]
    (println "🔍 Found" (count matching-paths) "paths containing '" substring "':")
    (doseq [path (take 10 matching-paths)]
      (println "  " path))
    (when (> (count matching-paths) 10)
      (println "  ... and" (- (count matching-paths) 10) "more"))
    matching-paths))

;; Resolve a $ref (string, keyword, or map) to actual schema
(defn resolve-ref [ref]
  (try
    (let [ref-str (cond
                    (string? ref) ref
                    (keyword? ref) (name ref)
                    (map? ref) (or (:$ref ref) (get ref "$ref"))
                    :else nil)
          path-parts (when (and ref-str (string? ref-str))
                       (->> (str/replace ref-str #"^#/" "")
                            (str/split #"/")
                            (remove empty?)
                            (mapv keyword)))]
      (when (seq path-parts)
        (get-in @github-api-spec path-parts)))
    (catch Exception e
      (println "❌ Error resolving ref:" ref "Error:" (.getMessage e))
      nil)))

;; Recursively resolve all $refs in a schema
(defn resolve-all-refs [schema]
  (cond
    (map? schema)
    (if (:$ref schema)
      (if-let [resolved (resolve-ref (:$ref schema))]
        (resolve-all-refs resolved)
        schema)
      (into {} (map (fn [[k v]] [k (resolve-all-refs v)]) schema)))
    
    (sequential? schema)
    (mapv resolve-all-refs schema)
    
    :else schema))

;; Inspect raw responses for a given API path
(defn inspect-path [path]
  (let [path-key (if (string? path) (keyword path) path)]
    (if-let [responses (get-in @github-api-spec [:paths path-key :get :responses])]
      (do
        (println "✅ Found :responses section for" path ". Keys are:")
        (pprint (keys responses))
        responses)
      (do
        (println "❌ Path not found or missing :responses for:" path)
        nil))))

;; Show resolved schema for a given path
(defn find-path-key [path]
  (some #(when (= (str %) (str ":" (if (.startsWith path "/") path (str "/" path))))
           %)
        (keys (:paths @github-api-spec))))

(defn show-schema [path]
  (let [path-key (find-path-key path)
        path-spec (get-in @github-api-spec [:paths path-key])]
    (cond
      (nil? path-key)
      (do (println "❌ Path not found:" path) false)

      (nil? (:get path-spec))
      (do (println "❌ No :get operation found for path:" path) false)

      :else
      (let [responses (:responses (:get path-spec))
            status-code (or (some #(when (contains? responses %) %) [:200 :201 "200" "201"])
                            (first (keys responses)))
            content (get-in responses [status-code :content])
            json-content (or (get content :application/json) (get content "application/json"))
            schema (:schema json-content)]
        (if schema
          (do (println "✅ Schema for path:" path)
              (pprint schema)
              schema)
          (do (println "❌ No application/json content-type under:" status-code)
              false))))))

;; Generate complete JSON schema for a path
(defn generate-json-schema [path]
  (let [path-key (if (string? path) (keyword path) path)]
    (if-let [schema (get-in @github-api-spec
                            [:paths path-key :get :responses :200 :content "application/json" :schema])]
      (let [resolved-schema (resolve-all-refs schema)
            json-schema (merge resolved-schema
                              {:$schema "https://json-schema.org/draft/2020-12/schema"
                               :$id (str "https://api.github.com/schemas" path)
                               :title (str "GitHub API Schema - " path)})]
        (json/generate-string json-schema {:pretty true}))
      nil)))

(defn- kget-in
  "Fixed version that handles the empty? error"
  [m ks]
  (let [kw-path (mapv keyword ks)
        st-path (mapv str ks)]
    (or (get-in m kw-path)
        (get-in m st-path))))

(defn list-schema-names []
  "Fixed version using the corrected kget-in"
  (let [schemas (or (kget-in @github-api-spec [:components :schemas]) {})]
    (->> (keys schemas)
         (map #(if (keyword? %) (name %) (str %)))
         sort
         vec)))

(defn get-schema
  "Fixed version of get-schema"
  [schema-id]
  (let [schemas (or (kget-in @github-api-spec [:components :schemas]) {})
        wanted  (cond
                  (string? schema-id)
                  (-> schema-id
                      (str/replace #"^#\/components\/schemas\/" "")
                      str/trim)
                  (keyword? schema-id) (name schema-id)
                  :else nil)]
    (if-not wanted
      (do (println "❌ Unsupported schema id:" (pr-str schema-id)) nil)
      (let [direct (or (get schemas (keyword wanted))
                       (get schemas wanted))
            ci     (when-not direct
                     (let [ci (str/lower-case wanted)]
                       (some (fn [k]
                               (when (= (str/lower-case (if (keyword? k) (name k) (str k))) ci)
                                 (get schemas k)))
                             (keys schemas))))]
        (cond
          direct (do (println "✅ Schema found:" wanted) direct)
          ci     (do (println "✅ Schema (case-insensitive) found:" wanted) ci)
          :else  (do (println "❌ Schema not found:" wanted)
                     (println "ℹ️ Available schema names:"
                              (take 10 (list-schema-names)))
                     nil))))))

(defn resolve-ref
  "Fixed version of resolve-ref"
  [ref]
  (try
    (let [ref-str (cond
                    (string? ref) ref
                    (keyword? ref) (name ref)
                    (map? ref) (let [v (or (get ref "$ref") (get ref :$ref))]
                                 (if (keyword? v) (name v) (str v)))
                    :else nil)]
      (when (and ref-str (not-empty ref-str))
        (let [parts (-> ref-str
                        (str/replace #"^#/" "")
                        (str/split #"/")
                        (->> (remove #(or (nil? %) (= % ""))))  ; Fixed: proper filtering
                        vec)]
          (when (seq parts)
            (kget-in @github-api-spec parts)))))
    (catch Exception e
      (println "❌ Error resolving ref:" (pr-str ref) "->" (.getMessage e))
      nil)))

;; Function to inline all $ref references in a schema



(defn inline-all-refs
  "Recursively inline all $ref references in a schema.
   visited-refs is a set to prevent infinite loops from circular references."
  ([schema] (inline-all-refs schema #{}))
  ([schema visited-refs]
   (cond
     ;; If it's a map with $ref, resolve and inline it
     (and (map? schema) (or (:$ref schema) (get schema "$ref")))
     (let [ref-str (or (:$ref schema) (get schema "$ref"))]
       (if (contains? visited-refs ref-str)
         ;; Circular reference detected - return a placeholder or the ref itself
         (do
           (println "⚠️  Circular reference detected:" ref-str)
           {:type "object" :description (str "Circular reference to " ref-str)})
         ;; Resolve and inline the reference
         (if-let [resolved (resolve-ref ref-str)]
           (inline-all-refs resolved (conj visited-refs ref-str))
           ;; If resolution fails, keep the original ref
           schema)))

     ;; If it's a map without $ref, recursively process all values
     (map? schema)
     (into {} (map (fn [[k v]] [k (inline-all-refs v visited-refs)]) schema))

     ;; If it's a vector/list, recursively process all elements
     (sequential? schema)
     (mapv #(inline-all-refs % visited-refs) schema)

     ;; Otherwise, return as-is (primitives, etc.)
     :else schema)))

(defn get-schema-inline
  "Get a schema with all $ref references resolved and inlined.
   This recursively replaces all {:$ref \"...\"} with their actual schema content."
  [schema-id]
  (let [base-schema (get-schema schema-id)]
    (if base-schema
      (inline-all-refs base-schema #{})  ; Use a set to track circular refs
      nil)))

(defn get-schema-inline-pretty
  "Get a schema with all refs inlined and pretty-print it"
  [schema-id]
  (if-let [inlined (get-schema-inline schema-id)]
    (do
      (println "✅ Inlined schema for:" schema-id)
      (pprint inlined)
      inlined)
    (do
      (println "❌ Failed to get schema:" schema-id)
      nil)))

(defn count-refs-in-schema
  "Count how many $ref references are in a schema (before inlining)"
  [schema]
  (cond
    (and (map? schema) (or (:$ref schema) (get schema "$ref")))
    1

    (map? schema)
    (reduce + (map count-refs-in-schema (vals schema)))

    (sequential? schema)
    (reduce + (map count-refs-in-schema schema))

    :else 0))

(defn inline-stats
  "Show statistics about inlining a schema"
  [schema-id]
  (if-let [original (get-schema schema-id)]
    (let [ref-count (count-refs-in-schema original)
          inlined (get-schema-inline schema-id)
          inlined-ref-count (if inlined (count-refs-in-schema inlined) 0)]
      (println "📊 Inline statistics for" schema-id ":")
      (println "  - Original $ref count:" ref-count)
      (println "  - Inlined $ref count:" inlined-ref-count)
      (println "  - Refs resolved:" (- ref-count inlined-ref-count))
      {:original-refs ref-count
       :remaining-refs inlined-ref-count
       :resolved-refs (- ref-count inlined-ref-count)})
    (println "❌ Schema not found:" schema-id)))

(defn get-schema-json-inline
  "Get a schema with all refs inlined as JSON string"
  [schema-id]
  (if-let [inlined (get-schema-inline schema-id)]
    (json/generate-string inlined {:pretty true})
    nil))

;; Example usage functions
(defn demo-inline-schema
  "Demonstrate inlining for a specific schema"
  [schema-id]
  (println "🎯 Demo: Inlining schema" schema-id)
  (println "\n1. Original schema:")
  (if-let [original (get-schema schema-id)]
    (pprint original)
    (println "❌ Schema not found"))
  
  (println "\n2. Inline statistics:")
  (inline-stats schema-id)
  
  (println "\n3. Fully inlined schema:")
  (get-schema-inline-pretty schema-id))

;; Utility to find schemas with the most references
(defn find-schemas-with-most-refs
  "Find schemas that have the most $ref references (good candidates for inlining)"
  ([]
   (find-schemas-with-most-refs 10))
  ([limit]
   (let [schema-names (list-schema-names)
         ref-counts (map (fn [name]
                          (if-let [schema (get-schema name)]
                            [name (count-refs-in-schema schema)]
                            [name 0]))
                        schema-names)
         sorted-counts (reverse (sort-by second ref-counts))]
     (println "🔗 Schemas with most $ref references:")
     (doseq [[name count] (take limit sorted-counts)]
       (println (format "  - %-30s: %d refs" name count)))
     (take limit sorted-counts))))

;; Test if inlining removes all references
(defn verify-inlining
  "Verify that inlining actually removes all $ref references"
  [schema-id]
  (if-let [inlined (get-schema-inline schema-id)]
    (let [remaining-refs (count-refs-in-schema inlined)]
      (if (zero? remaining-refs)
        (println "✅ All references successfully inlined for" schema-id)
        (println "⚠️ " remaining-refs "references still remain in" schema-id))
      (zero? remaining-refs))
    (do
      (println "❌ Failed to inline schema:" schema-id)
      false)))

;; Debug functions
(defn debug-spec []
  (if @github-api-spec
    (do
      (println "📊 GitHub API Spec Status:")
      (println "  ✅ Spec is loaded")
      (println "  📋 OpenAPI version:" (:openapi @github-api-spec))
      (println "  📋 API title:" (get-in @github-api-spec [:info :title]))
      (println "  📋 Total paths:" (count (:paths @github-api-spec)))
      (println "  📋 Schema components:" (count (get-in @github-api-spec [:components :schemas])))
      
      (println "\n🔍 Sample paths:")
      (doseq [path (take 5 (keys (:paths @github-api-spec)))]
        (println "   " path))
      true)
    (do
      (println "❌ No GitHub API spec loaded")
      false)))

(defn debug-path [path]
  (let [path-key (if (string? path) (keyword path) path)]
    (println "🔍 Debug info for path:" path)
    (println "  Path key:" path-key)
    (println "  Path exists in :paths?" (contains? (:paths @github-api-spec) path-key))
    
    (when-let [path-data (get (:paths @github-api-spec) path-key)]
      (println "  Available methods:" (keys path-data))
      
      (when-let [get-data (:get path-data)]
        (println "  GET method keys:" (keys get-data))
        
        (when-let [responses (:responses get-data)]
          (println "  Response codes:" (keys responses))
          
          (when-let [resp-200 (:200 responses)]
            (println "  200 response structure:" (keys resp-200))
            
            (when-let [content (:content resp-200)]
              (println "  Content types:" (keys content))
              
              (when-let [json-content (get content "application/json")]
                (println "  JSON schema keys:" (keys json-content))))))))))

;; =============================================================================
;; READY-TO-USE TEST FUNCTIONS
;; =============================================================================

(defn test-github-api []
  (println "🚀 Testing GitHub API Schema Loading and Parsing")
  (println "=" (apply str (repeat 60 "=")))
  
  ;; 1. Load the spec
  (println "\n1️⃣ Loading GitHub OpenAPI spec...")
  (if (load-spec! "https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json")
    (do
      ;; 2. Debug spec
      (println "\n2️⃣ Spec debug info:")
      (debug-spec)
      
      ;; 3. Test path existence
      (println "\n3️⃣ Testing path existence:")
      (path-exists? "/repos/{owner}/{repo}/issues")
      (path-exists? "/user/repos")
      
      ;; 4. List matching paths
      (println "\n4️⃣ Finding issue-related paths:")
      (list-matching-paths "issues")
      
      ;; 5. Inspect specific path
      (println "\n5️⃣ Inspecting issues path:")
      (debug-path "/repos/{owner}/{repo}/issues")
      
      ;; 6. Show schema
      (println "\n6️⃣ Showing complete schema:")
      (show-schema "/repos/{owner}/{repo}/issues")
      
      ;; 7. Generate JSON schema
      (println "\n7️⃣ Generating JSON Schema:")
      (if-let [json-schema (generate-json-schema "/repos/{owner}/{repo}/issues")]
        (do
          (println "✅ JSON Schema generated successfully!")
          (println "📏 Schema length:" (count json-schema) "characters")
          (println "🎯 First 500 characters:")
          (println (subs json-schema 0 (min 500 (count json-schema)))))
        (println "❌ Failed to generate JSON schema"))
      
      (println "\n✅ Test completed!"))
    
    (println "❌ Failed to load spec, test aborted.")))

;; Quick helpers for common tasks
(defn quick-test []
  (println "⚡ Quick Test")
  (load-spec! "https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.json")
  (show-schema "/repos/{owner}/{repo}/issues"))

(defn save-json-schema [path filename]
  (if-let [json-schema (generate-json-schema path)]
    (do
      (spit filename json-schema)
      (println "✅ JSON schema saved to:" filename))
    (println "❌ Failed to generate schema for:" path)))

;; Print usage instructions
(println "🎉 GitHub Schema Tester Loaded!")
(println "\nQuick start:")
(println "  (test-github-api)           ; Run complete test suite")
(println "  (quick-test)                ; Quick test")
(println "  (debug-spec)                ; Check if spec is loaded")
(println "")
(println "Main functions:")
(println "  (load-spec! url-or-file)    ; Load OpenAPI spec")
(println "  (show-schema path)          ; Show resolved schema")
(println "  (generate-json-schema path) ; Generate JSON Schema")
(println "  (save-json-schema path file); Save JSON Schema to file")
(println "")
(println "Ready to test! Run (test-github-api) to start! 🚀")
