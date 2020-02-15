(ns toucandb.store)

(defonce base-store (atom {}))

(defonce port->transaction-stores (atom {}))

(defn value [client-id k]
  (some
   (fn [store]
     (get store k))
   (concat (get @port->transaction-stores client-id)
           [@base-store])))

(defn set-value! [client-id k v]
  (if (seq (get @port->transaction-stores client-id))
    (swap! port->transaction-stores update client-id (fn [[store & more]]
                                                       (cons (assoc store k v) more)))
    (swap! base-store assoc k v))
  nil)

(defn delete! [client-id k]
  (set-value! client-id k nil)
  nil)

(defn begin! [client-id]
  (swap! port->transaction-stores update client-id conj {})
  nil)

(defn commit! [client-id]
  (let [stores (get @port->transaction-stores client-id)]
    (assert (seq stores) "No open transactions!")
    (if (= (count stores) 1)
      (do
        (swap! base-store merge (first stores))
        (swap! port->transaction-stores dissoc client-id))
      (do
        (swap! port->transaction-stores assoc client-id (cons (merge (second stores) (first stores))
                                                              (drop 2 stores))))))
  nil)

(defn rollback! [client-id]
  (assert (seq (get @port->transaction-stores client-id)) "No open transactions")
  (swap! port->transaction-stores update client-id rest)
  nil)
