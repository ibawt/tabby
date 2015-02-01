(ns tabby.utils)

(defmacro dbg [& body]
  `(let [x# ~body]
     (println (quote ~body) "=" x#) x#))

(def election-timeout-max 150)

(defn valid-term? [{c-t :current-term} {term :term}]
  (>= term c-t))

(defn random-election-timeout
  "returns election-timeout-max + rand(0, election-timeout-max)"
  []
  (+ (rand-int election-timeout-max) election-timeout-max))

(defn foreach-peer [state f & args]
  "calls f with current state, the peer and etc."
  (loop [s state
         p (:peers state)]
    (if (empty? p) s
        (recur (apply f s (first p) args)
               (rest p)))))

(defn transmit [state request]
  (update-in state [:tx-queue] conj request))

(defn quorum? [state c]
  (>= c (inc (/ (count (:peers state)) 2))))
