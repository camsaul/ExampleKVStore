(ns toucandb.core
  (:gen-class)
  (:require [clojure.core.async :as a]
            [toucandb.cmd :as cmd])
  (:import [java.io BufferedReader BufferedWriter InputStreamReader OutputStreamWriter Writer]
           [java.net ServerSocket Socket]))

(set! *warn-on-reflection* true)

(def ^:private client-timeout-ms (* 10 60 1000))
(def ^:private response-timeout-ms 1000)

(declare listen*)

(defn- respond* [input {:keys [close! port ^Writer writer], :as context}]
  (letfn [(respond! [format-string & args]
            (when (some? format-string)
              (.write writer (str (apply format format-string args) "\n"))
              (.flush writer))
            (listen* context))]
    (a/go
      (let [response-timeout-chan (a/timeout response-timeout-ms)
            [val port]            (a/alts! [response-timeout-chan
                                            (a/thread
                                              (try
                                                (cmd/handle-command port input)
                                                (catch Throwable e
                                                  e)))])]
        (cond
          (= port response-timeout-chan)
          (respond! "Failed to generate response after %d ms" response-timeout-ms)

          (:close? (ex-data val))
          (close! "Client typed EXIT command")

          (instance? Throwable val)
          (respond! "Error: %s" (.getMessage ^Throwable val))

          :else
          (respond! (when (some? val) (with-out-str (print val)))))))))

(defn- listen* [{:keys [close! server-closed-chan port ^Socket socket ^BufferedReader reader ^Writer writer], :as context}]
  (a/go
    (let [timeout-chan (a/timeout client-timeout-ms)
          [val port]   (a/alts! [server-closed-chan
                                 (a/thread
                                   (try
                                     (.write writer "> ")
                                     (.flush writer)
                                     (.readLine reader)
                                     (catch Throwable e
                                       e)))
                                 timeout-chan])]
      (cond
        (= port server-closed-chan)
        (close! "Parent server socket is closed.")

        (= port timeout-chan)
        (close! "Timed out after %d ms.\n" client-timeout-ms)

        (nil? val)
        (close! "Connection closed by client." port)

        (instance? Throwable val)
        (close! "Client got Exception: %s" val)

        :else
        (respond* val context)))))

(defn- start-client-loop! [server-closed-chan ^Socket socket]
  (printf "Opened new client connection on port %d\n" (.getPort socket))
  (.setKeepAlive socket true)
  (let [reader  (BufferedReader. (InputStreamReader. (.getInputStream socket)))
        writer  (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket)))
        context {:socket             socket
                 :server-closed-chan server-closed-chan
                 :port               (.getPort socket)
                 :reader             reader
                 :writer             writer
                 :close!             (fn [format-str & args]
                                       (println (format "Stopping client loop on port %d:" (.getPort socket))
                                                (apply format format-str args))
                                       (.close reader)
                                       (.close writer)
                                       (.close socket))}]
    (listen* context)))

(defn- start-accept-loop! [^ServerSocket socket]
  (let [server-closed-chan (a/promise-chan)
        close!             (fn [format-string & args]
                             (println "Stopping accept loop:" (apply format format-string args))
                             (a/>!! server-closed-chan ::close)
                             (.close socket))]
    (a/go-loop []
      (let [val (a/<! (a/thread
                        (try
                          (.accept socket)
                          (catch Throwable e
                            e))))]
        (cond
          (.isClosed socket)
          (close! "Server socket is closed.")

          (instance? Throwable val)
          (close! "Server socket threw exception %s" val)

          :else
          (do
            (start-client-loop! server-closed-chan val)
            (recur)))))))

(defonce ^:private socket* (atom nil))

(defn- server-socket
  (^ServerSocket []
   @server-socket)

  ;; thread-safe setter
  (^ServerSocket [^ServerSocket new-socket]
   (let [[^ServerSocket old new] (reset-vals! socket* new-socket)]
     (when old
       (println "Closing server socket" old)
       (.close old))
     (when-not (= new-socket new)
       (println "Closing server socket" new-socket)
       (.close new-socket))
     (println "New server socket:" new)
     new)))

(defn- stop! []
  (server-socket nil))

(defn- start! [^Integer port]
  (println "Starting server on port" port)
  (server-socket nil)
  (let [server-socket (server-socket (ServerSocket. port))]
    (start-accept-loop! server-socket))
  nil)

(defn -main [port]
  (start! (Integer/parseUnsignedInt port))
  (loop []
    (Thread/sleep 1000)
    (recur)))
