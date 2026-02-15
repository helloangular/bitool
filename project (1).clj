(defproject bitool "0.1.0-SNAPSHOT"



  :description "FIXME: write description"
  :url "http://example.com/FIXME"

  :dependencies [[ch.qos.logback/logback-classic "1.4.4"]
                 [clojure.java-time "1.1.0"]
;;                 [org.gershwinlang/gershwin "0.2.0"]
                 [cprop "0.1.19"]
                 [expound "0.9.0"]
                 [funcool/struct "1.4.0"]
                 [json-html "0.4.7"]
                 [luminus-transit "0.1.5"]
                 [luminus-undertow "0.1.18"]
                 [luminus/ring-ttl-session "0.3.3"]
                 [markdown-clj "1.11.3"]
                 [metosin/muuntaja "0.6.8"]
                 [metosin/reitit "0.5.18"]
                 [metosin/ring-http-response "0.9.3"]
                 [mount "0.1.16"]
                 [nrepl "1.0.0"]
                 [org.clojure/clojure "1.12.0"]
                 [org.clojure/tools.cli "1.0.214"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.webjars.npm/bulma "0.9.4"]
                 [org.webjars.npm/material-icons "1.10.8"]
                 [org.webjars/webjars-locator "0.45"]
                 [org.webjars/webjars-locator-jboss-vfs "0.1.0"]
                 [ring-webjars "0.2.0"]
                 [ring/ring-core "1.9.6"]
                 [ring/ring-defaults "0.3.4"]
                 [ring/ring-json "0.5.1"]
                 [ubergraph "0.8.2"]
		 [org.clojure/java.jdbc "0.7.12"]
                 [io.github.camsaul/toucan2 "1.0.535"]
;;                 [org.postgresql/postgresql "42.3.1"]
;;		  [com.cognitect/hmac-authn "0.1.211"]
;;		  [com.cognitect/http-client "1.0.126"]
;;		 [com.datomic/client-pro "1.0.77"]
;;                 [com.datomic/local "1.0.276"]
;;                 [org.apache.commons/commons-io "2.11.0"]
                 [commons-codec/commons-codec "1.15"]
                 [clj-http "3.12.3"]
                 [cheshire "5.10.0"] 
                 [buddy/buddy-core "1.12.0-430"]
		 [aysylu/loom "1.0.2"]
                 [com.rpl/specter "1.1.4"]
                 [com.zaxxer/HikariCP "6.2.1"]  
                 [io.github.camsaul/toucan2 "1.0.556"]
                 [org.clojure/tools.namespace "1.5.0"]
                 [org.clojure/data.codec "0.1.1"]
                 [com.taoensso/telemere "1.0.1"]
                 [com.taoensso/timbre "6.5.0"]
                 [org.clojure/core.async "1.7.701"]


;;                 [com.datomic/peer "1.0.7075"]
                 [com.github.seancorfield/next.jdbc "1.3.955"]
                 [org.postgresql/postgresql "42.2.10"]
                 [com.github.seancorfield/honeysql "2.3.928"]

                 [while-let "0.2.0"]
                 [selmer "1.12.55"]]

;;  :repositories {"local" "/Users/harishkulkarni/.m2/repository"}
;;  :repositories [["clojars" "https://repo.clojars.org/"]]

  :min-lein-version "2.0.0"
  
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :resource-paths ["resources"]
  :target-path "target/%s/"
  :main ^:skip-aot bitool.core

  :plugins [] 

  :profiles
  {:uberjar {:omit-source true
             :aot :all
             :uberjar-name "bitool.jar"
             :source-paths ["env/prod/clj" ]
             :resource-paths ["env/prod/resources"]}

   :dev           [:project/dev :profiles/dev]
   :test          [:project/dev :project/test :profiles/test]

   :project/dev  {:jvm-opts ["-Dconf=dev-config.edn" ]
                  :dependencies [[org.clojure/tools.namespace "1.3.0"]
                                 [pjstadig/humane-test-output "0.11.0"]
                                 [prone "2021-04-23"]
                                 [ring/ring-devel "1.9.6"]
                                 [ring/ring-mock "0.4.0"]]
                  :plugins      [[com.jakemccrary/lein-test-refresh "0.24.1"]
                                 [jonase/eastwood "1.2.4"]
                                 [cider/cider-nrepl "0.26.0"]] 
                  
                  :source-paths ["env/dev/clj" ]
                  :resource-paths ["env/dev/resources"]
                  :repl-options {:init-ns user
                                 :timeout 120000}
                  :injections [(require 'pjstadig.humane-test-output)
                               (pjstadig.humane-test-output/activate!)]}
   :project/test {:jvm-opts ["-Dconf=test-config.edn" ]
                  :resource-paths ["env/test/resources"] }
   :profiles/dev {}
   :profiles/test {}})
