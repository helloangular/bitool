(ns bitool.connector.schema-converter
  (:require [clojure.string :as str]
            [cheshire.core :as json]))

(defn format-type-info
  "Format type information from a schema property"
  [prop-schema]
  (let [type-str (or (:type prop-schema) "object")
        format-str (when (:format prop-schema)
                     (str ", " (:format prop-schema)))
        nullable-str (when (:nullable prop-schema) ", nullable")
        ref-str (when (:$ref prop-schema)
                  (str " - Reference: " (:$ref prop-schema)))
        enum-str (when (:enum prop-schema)
                   (str " - Values: " (str/join ", " (:enum prop-schema))))
        example-str (when (:example prop-schema)
                      (str " - Example: " (:example prop-schema)))]
    (str "(" type-str format-str nullable-str ")" ref-str enum-str example-str)))

(defn create-node
  "Create a tree node with id, label, and items"
  [id label & {:keys [expanded items] :or {expanded false items []}}]
  (cond-> {:id id :label label :items items}
    (some? expanded) (assoc :expanded expanded)))

(defn generate-node-id
  "Generate a unique node ID from parent path and property name"
  [parent-path prop-name]
  (if (empty? parent-path)
    prop-name
    (str parent-path "_" (str/replace prop-name #"[^a-zA-Z0-9_]" "_"))))

;; Forward declarations for mutually recursive functions
(declare process-properties)
(declare process-one-of)
(declare process-array-items)

(defn process-one-of
  "Process oneOf schema definitions"
  [one-of-schemas parent-path prop-name]
  (map-indexed
   (fn [idx schema]
     (let [node-id (generate-node-id parent-path (str prop-name "_option_" (inc idx)))
           type-info (format-type-info schema)
           label (str "Option " (inc idx) ": " type-info)]
       (if (:properties schema)
         (create-node node-id label
                      :items (process-properties (:properties schema) node-id))
         (create-node node-id label))))
   one-of-schemas))

(defn process-array-items
  "Process array items schema"
  [items-schema parent-path prop-name]
  (cond
    (:oneOf items-schema)
    (process-one-of (:oneOf items-schema) parent-path (str prop-name "_items"))

    (:$ref items-schema)
    [(create-node (generate-node-id parent-path (str prop-name "_items_ref"))
                  (str "Items Reference: " (:$ref items-schema)))]

    (:properties items-schema)
    (process-properties (:properties items-schema)
                       (generate-node-id parent-path (str prop-name "_items")))

    :else
    [(create-node (generate-node-id parent-path (str prop-name "_items"))
                  (str "Items: " (format-type-info items-schema)))]))


(defn process-properties
  "Process schema properties recursively"
  [properties parent-path]
  (map
   (fn [[prop-name prop-schema]]
     (let [node-id (generate-node-id parent-path (name prop-name))
           type-info (format-type-info prop-schema)
           description (when (:description prop-schema)
                        (str " - " (:description prop-schema)))
           base-label (str (name prop-name) " " type-info description)

           child-items (cond
                        ;; Handle oneOf
                        (:oneOf prop-schema)
                        (process-one-of (:oneOf prop-schema) node-id (name prop-name))

                        ;; Handle arrays with items
                        (and (= "array" (:type prop-schema)) (:items prop-schema))
                        (process-array-items (:items prop-schema) node-id (name prop-name))

                        ;; Handle objects with properties
                        (:properties prop-schema)
                        (process-properties (:properties prop-schema) node-id)

                        ;; Handle required fields
                        (:required prop-schema)
                        [(create-node (generate-node-id node-id "required")
                                     (str "Required fields: " (str/join ", " (:required prop-schema))))]

                        ;; Handle enum values
                        (:enum prop-schema)
                        [(create-node (generate-node-id node-id "enum")
                                     (str "Possible values: " (str/join ", " (:enum prop-schema))))]

                        :else [])]

       (create-node node-id base-label :items child-items)))
   properties))


(defn categorize-properties
  "Categorize properties into logical groups"
  [properties]
  (let [basic-info-keys #{:title :body :number :id :node_id :description}
        status-keys #{:state :state_reason :locked :active_lock_reason :draft}
        timestamp-keys #{:created_at :updated_at :closed_at}
        user-keys #{:user :assignee :assignees :author_association :closed_by}
        label-milestone-keys #{:labels :milestone}
        content-keys #{:body_html :body_text}
        interaction-keys #{:comments :reactions}
        url-keys #{:url :html_url :repository_url :labels_url :comments_url
                   :events_url :timeline_url}
        repository-keys #{:repository}
        pr-keys #{:pull_request}
        dependency-keys #{:issue_dependencies_summary :sub_issues_summary}
        integration-keys #{:performed_via_github_app :type}

        categorize-prop (fn [[k v]]
                         (cond
                          (basic-info-keys k) [:basic-info k v]
                          (status-keys k) [:status k v]
                          (timestamp-keys k) [:timestamps k v]
                          (user-keys k) [:users k v]
                          (label-milestone-keys k) [:labels-milestone k v]
                          (content-keys k) [:content k v]
                          (interaction-keys k) [:interactions k v]
                          (url-keys k) [:urls k v]
                          (repository-keys k) [:repository k v]
                          (pr-keys k) [:pull-request k v]
                          (dependency-keys k) [:dependencies k v]
                          (integration-keys k) [:integrations k v]
                          :else [:other k v]))]

 (->> properties
         (map categorize-prop)
         (group-by first)
         (map (fn [[category props]]
                [category (into {} (map (fn [[_ k v]] [k v]) props))]))
         (into {}))))

(defn create-category-node
  "Create a category node with its properties"
  [category-key category-name properties parent-path expanded?]
  (let [node-id (generate-node-id parent-path (name category-key))
        items (process-properties properties node-id)]
    (create-node node-id category-name :expanded expanded? :items items)))

(defn schema-to-tree
  "Convert a schema to Smart UI tree JSON format"
  [schema & {:keys [categorize?] :or {categorize? true}}]
  (let [root-title (or (:title schema) "Schema")
        root-id (str/replace (str/lower-case root-title) #"[^a-zA-Z0-9]" "_")
        properties (:properties schema)]

   (if (and categorize? (> (count properties) 10))
      ;; Categorized version for large schemas
      (let [categorized (categorize-properties properties)
            category-configs [[:basic-info "Basic Information" true]
                             [:status "Status & State" true]
                             [:timestamps "Timestamps" false]
                             [:users "Users & Assignments" false]
                             [:labels-milestone "Labels & Milestone" false]
                             [:content "Content & Display" false]
                             [:interactions "Interactions" false]
                             [:urls "URLs" false]
                             [:repository "Repository Information" false]
                             [:pull-request "Pull Request Information" false]
                             [:dependencies "Dependencies & Sub-issues" false]
                             [:integrations "Integrations" false]
                             [:other "Other Properties" false]]

            category-nodes (keep (fn [[cat-key cat-name expanded?]]
                                  (when-let [props (get categorized cat-key)]
                                    (create-category-node cat-key cat-name props root-id expanded?)))
                                category-configs)]

        [(create-node root-id root-title :expanded true :items category-nodes)])

      ;; Simple flat version for smaller schemas
      (let [items (process-properties properties root-id)]
        [(create-node root-id root-title :expanded true :items items)]))))

(defn schema-to-json
  "Convert schema to JSON string for frontend"
  [schema & options]
  (json/generate-string (apply schema-to-tree schema options) {:pretty true}))

;; Example usage and test functions
(defn test-converter
  "Test the converter with the issue schema"
  []
  (let [issue-schema {:title "Issue"
                      :description "Issues are a great way to keep track of tasks, enhancements, and bugs for your projects."
                      :type "object"
                      :properties {:title {:type "string"
                                          :description "Title of the issue"
                                          :example "Widget creation fails in Safari on OS X 10.8"}
                                  :state {:type "string"
                                         :description "State of the issue; either 'open' or 'closed'"
                                         :example "open"}
                                  :labels {:type "array"
                                          :description "Labels to associate with this issue"
                                          :items {:oneOf [{:type "string"}
                                                         {:type "object"
                                                          :properties {:id {:type "integer"}
                                                                      :name {:type "string"}
                                                                      :color {:type "string" :nullable true}}}]}}}}]
    (schema-to-json issue-schema)))

;; Utility functions
(defn save-json-to-file
  "Save the converted JSON to a file"
  [schema filename & options]
  (spit filename (apply schema-to-json schema options)))

(defn print-tree-summary
  "Print a summary of the tree structure"
  [schema]
  (let [tree (schema-to-tree schema)
        count-nodes (fn count-nodes [nodes]
                     (reduce + (count nodes)
                            (map #(count-nodes (:items %)) nodes)))]
    (println "Tree Summary:")
    (println "- Root nodes:" (count tree))
    (println "- Total nodes:" (count-nodes tree))
    (println "- Max depth:"
             (letfn [(max-depth [nodes depth]
                       (if (empty? nodes)
                         depth
                         (apply max (map #(max-depth (:items %) (inc depth)) nodes))))]
               (max-depth tree 0)))))

;; Example: Convert your issue schema
(comment
  ;; Your issue-schema would be defined here
  (def issue-schema
    {:title "Issue"
     :description "Issues are a great way to keep track of tasks, enhancements, and bugs for your projects."
     :type "object"
     :properties {;; ... your full schema here
                  }
     :required [;; ... required fields
                ]})

  ;; Convert to JSON
  (def tree-json (schema-to-json issue-schema))

  ;; Save to file
  (save-json-to-file issue-schema "issue-tree.json")

  ;; Print summary
  (print-tree-summary issue-schema)

  ;; Convert without categorization (flat structure)
  (def flat-json (schema-to-json issue-schema :categorize? false)))
