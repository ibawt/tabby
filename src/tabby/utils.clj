(ns tabby.utils)

(defmacro update
  ([m k f]
   `(update-in ~m [~k] ~f))
  ([m k f & args]
   `(update-in ~m [~k] ~f ~@args)))

(defmacro dbg [& body]
  `(let [x# ~body]
     (println (quote ~body) "=" x#) x#))

(defn mapf
  "apply f to each value of m and return the updated m"
  ([m f]
   (into {} (for [[k v] m] [k (f v)])))
  ([m f arg0]
   (into {} (for [[k v] m] [k (f v arg0)])))
  ([m f arg0 & args]
   (into {} (for [[k v] m] [k (apply f v arg0 args)]))))

(def election-timeout-max 150)

(defn valid-term? [{c-t :current-term} {term :term}]
  (>= term c-t))

(defn random-election-timeout
  "returns election-timeout-max + rand(0, election-timeout-max)"
  []
  (+ (rand-int election-timeout-max) election-timeout-max))

(defn foreach-peer
  "calls f with current state, the peer and etc."
  [state f & args]
  (loop [s state
         p (:peers state)]
    (if (empty? p) s
        (recur (apply f s (first p) args)
               (rest p)))))

(defn transmit [state request]
  (update state :tx-queue conj request))

(defn quorum? [state c]
  (>= c (inc (/ (count (:peers state)) 2))))

(defn leader? [state]
  (= :leader (:type state)))

(defn follower? [state]
  (= :follower (:type state)))

(defn candidate? [state]
  (= :candidate (:type state)))

(defn if-leader? [state f & args]
  (if (leader? state) (apply f state args) state))

(defn if-follower? [state f & args]
  (if (follower? state) (apply f state args) state))

(defn if-not-leader? [state f & args]
  (if-not (leader? state) (apply f state args) state))
