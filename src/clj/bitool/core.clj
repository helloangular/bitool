(ns bitool.core
  (:require
    [bitool.config :as config]
    [bitool.control-plane]
    [bitool.handler :as handler]
    [bitool.ingest.execution]
    [bitool.ingest.scheduler]
    [bitool.lifecycle :as lifecycle]
    [bitool.modeling.automation]
    [bitool.operations]
    [bitool.nrepl :as nrepl]
    [luminus.http-server :as http]
    [bitool.config :refer [env]]
    [clojure.tools.cli :refer [parse-opts]]
    [clojure.tools.logging :as log]
    [mount.core :as mount])
  (:gen-class))

;; log uncaught exceptions in threads
(Thread/setDefaultUncaughtExceptionHandler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread ex]
      (log/error {:what :uncaught-exception
                  :exception ex
                  :where (str "Uncaught exception on" (.getName thread))}))))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :parse-fn #(Integer/parseInt %)]])

(mount/defstate ^{:on-reload :noop} http-server
  :start
  (when (config/enabled-role? :api)
    (http/start
      (-> env
          (update :io-threads #(or % (* 2 (.availableProcessors (Runtime/getRuntime))))) 
          (assoc  :handler (handler/app))
          (update :port #(or (-> env :options :port) %))
          (select-keys [:io-threads :handler :host :port :async?]))))
  :stop
  (when http-server
    (http/stop http-server)))

(mount/defstate ^{:on-reload :noop} repl-server
  :start
  (when (:nrepl-port env)
    (nrepl/start {:bind (:nrepl-bind env)
                  :port (:nrepl-port env)}))
  :stop
  (when repl-server
    (nrepl/stop repl-server)))


(defn stop-app []
  (lifecycle/mark-draining!)
  (let [drain-ms (lifecycle/shutdown-drain-ms)]
    (when (pos? drain-ms)
      (log/info "Graceful shutdown: entering drain mode" {:drain_ms drain-ms})
      (Thread/sleep drain-ms)))
  (doseq [component (:stopped (mount/stop))]
    (log/info component "stopped"))
  (shutdown-agents))

(defn start-app [args]
  (lifecycle/clear-draining!)
  (doseq [component (-> args
                        (parse-opts cli-options)
                        mount/start-with-args
                        :started)]
    (log/info component "started"))
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-app)))

(defn -main [& args]
  (start-app args))
