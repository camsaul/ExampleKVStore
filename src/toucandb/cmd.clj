(ns toucandb.cmd
  (:require [clojure.string :as str]
            [toucandb.store :as store]))


(defmulti ^:private do!
  {:arglists '([client-id command & args])}
  (fn [_ command & args]
    [(str/upper-case command)
     (count args)]))

(defmethod do! :default
  [_ command & _]
  "Invalid command.")

(defmethod do! ["SET" 2]
  [client-id _ k v]
  (store/set-value! client-id k v))

(defmethod do! ["GET" 1]
  [client-id _ k]
  (store/value client-id k))

(defmethod do! ["DELETE" 1]
  [client-id _ k]
  (store/delete! client-id k))

(defmethod do! ["BEGIN" 0]
  [client-id _]
  (store/begin! client-id))

(defmethod do! ["ROLLBACK" 0]
  [client-id _]
  (store/rollback! client-id))

(defmethod do! ["COMMIT" 0]
  [client-id _]
  (store/commit! client-id))

(defmethod do! ["EXIT" 0]
  [_ _]
  (throw (ex-info "Goodbye!" {:close? true})))

(defn- parse [input]
  (str/split input #"\s+"))

(defn handle-command [port input]
  (printf "Client at port %d got line: %s\n" port (pr-str (parse input)))
  (let [result (apply do! port (parse input))]
    (println "base-store" (pr-str @store/base-store))
    (println "port->transaction-stores:" (pr-str @store/port->transaction-stores))
    result))
