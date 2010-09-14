(ns redis.internal
  (:refer-clojure :exclude [send read read-line])
  (:use clojure.java.io)
  (:import [java.io InputStream BufferedInputStream]
           [java.net Socket]))

(defstruct connection
  :host :port :password :db :timeout :socket :istream :writer)

(def *connection* (struct-map connection
                    :host     "127.0.0.1"
                    :port     6379
                    :password nil
                    :db       0
                    :timeout  5000
                    :socket   nil
                    :istream   nil
                    :writer   nil))

(def *binary-strings* false)

(def *cr*  0x0d)
(def *lf*  0x0a)
(defn- cr? [c] (= c *cr*))
(defn- lf? [c] (= c *lf*))

(defn- uppercase [#^String s] (.toUpperCase s))
(defn- trim [#^String s] (.trim s))
(defn- parse-int [#^String s] (Integer/parseInt s))
;(defn- char-array [len] (make-array Character/TYPE len))

(defn connect-to-server
  "Create a Socket connected to server"
  [server]
  (let [{:keys [host port timeout]} server
        socket (Socket. #^String host #^Integer port)]
    (doto socket
      (.setTcpNoDelay true)
      (.setKeepAlive true))))
 
(defn with-server*
  [server-spec func]
  (let [connection (merge *connection* server-spec)]
    (with-open [#^Socket socket (connect-to-server connection)]
      (let [input-stream (.getInputStream socket)
            output-stream (.getOutputStream socket)
            input-stream (BufferedInputStream. input-stream)]
        (binding [*connection* (assoc connection 
                                 :socket socket
                                 :istream input-stream)]
          (func))))))
 
(defn socket* []
  (or (:socket *connection*)
      (throw (Exception. "Not connected to a Redis server"))))

(defn send-command
  "Send a command string to server"
  [cmd]
  (let [out (.getOutputStream (#^Socket socket*))]
    (copy cmd out)))
   
(defn read-line-crlf
  "Read from reader until exactly a CR+LF combination is
  found. Returns the line read without trailing CR+LF.
 
  This is used instead of Reader.readLine() method since that method
  tries to read either a CR, a LF or a CR+LF, which we don't want in
  this case."
  [^InputStream istream]
    (loop [line []]
      (let [b (.read istream)]
	(when (< b 0)
	  (throw (Exception. 
		  "Error reading line: EOF reached before CR/LF sequence")))
	(if (cr? b)
	  (let [next (.read istream)]
	    (if (lf? next)
	      line
	      (recur (conj (conj line b) next))))
	  (recur (conj line b))))))
 
(defn  read-line-crlf-string
  [^InputStream istream]
  (apply str (map char (read-line-crlf istream))))


;;
;; Reply dispatching
;;
(defn reply-type
  ([^InputStream istream]
     (char (.read istream))))

(defmulti parse-reply reply-type :default :unknown)

(defn read-reply
  ([]
     (let [istream (*connection* :istream)]
       (read-reply istream)))
  ([^InputStream istream]
     (parse-reply istream)))

(defmethod parse-reply :unknown
  [^InputStream istream]
  (throw (Exception. (str "Unknown reply type:"))))
 
(defmethod parse-reply \-
  [^InputStream istream]
  (let [error (read-line-crlf-string istream)]
    (str "Server error: " error)))
 
(defmethod parse-reply \+
  [^InputStream istream]
  (read-line-crlf-string istream))

(defmethod parse-reply \$
  [^InputStream istream]
  (let [line (read-line-crlf-string istream)
        length (parse-int line)]
    (if (< length 0)
      nil
      (let [buf (byte-array length)]
	(loop [total-read 0]
	  (let [total-read 
		(+ total-read (.read istream buf total-read (- length total-read)))]
	    (if (= total-read length)
	      (if *binary-strings* buf (String. buf))
	      (recur total-read))))))))


(defmethod parse-reply \*
  [^InputStream istream]
  (let [line (read-line-crlf-string istream)
        count (parse-int line)]
    (if (< count 0)
      nil
      (loop [i count
             replies []]
        (if (zero? i)
          replies
          (recur (dec i) (conj replies (read-reply istream))))))))
 
(defmethod parse-reply \:
  [^InputStream istream]
  (let [line (trim (read-line-crlf-string istream))
        int (parse-int line)]
    int))
 

;;
;; Command functions
;;
(defn- str-join
  "Join elements in sequence with separator"
  [separator sequence]
  (apply str (interpose separator sequence)))


(defn inline-command
  "Create a string for an inline command"
  [name & args]
  (let [cmd (str-join " " (conj args name))]
    (str cmd "\r\n")))

(defn bulk-command
  "Create a string for a bulk command"
  [name & args]
  (let [data (last args)
        data-length (count data)
        args* (concat [name] (butlast args) [data-length])]
    (with-open [baos (java.io.ByteArrayOutputStream.)]
      (copy (str-join " " args*) baos)
      (copy "\r\n" baos)
      (copy data baos)
      (copy "\r\n" baos)
      (if *binary-strings*
	(.toByteArray baos)
	(String. (.toByteArray baos))))))

(defn- sort-command-args-to-string
  [args]
  (loop [arg-strings []
         args args]
    (if (empty? args)
      (str-join " " arg-strings)
      (let [type (first args)
            args (rest args)]
        (condp = type
          :by (let [pattern (first args)]
                (recur (conj arg-strings "BY" pattern)
                       (rest args)))
          :limit (let [start (first args)
                       end (second args)]
                   (recur (conj arg-strings "LIMIT" start end)
                          (drop 2 args)))
          :get (let [pattern (first args)]
                 (recur (conj arg-strings "GET" pattern)
                        (rest args)))
          :store (let [key (first args)]
                   (recur (conj arg-strings "STORE" key)
                          (rest args)))
          :alpha (recur (conj arg-strings "ALPHA") args)
          :asc  (recur (conj arg-strings "ASC") args)
          :desc (recur (conj arg-strings "DESC") args)
          (throw (Exception. (str "Error parsing SORT arguments: Unknown argument: " type))))))))

(defn sort-command
  [name & args]
  (when-not (= name "SORT")
    (throw (Exception. "Sort command name must be 'SORT'")))
  (let [key (first args)
        arg-string (sort-command-args-to-string (rest args))
        cmd (str "SORT " key)]
    (if (empty? arg-string)
      (str cmd "\r\n")
      (str cmd " " arg-string "\r\n"))))


(def command-fns {:inline 'inline-command
                  :bulk   'bulk-command
                  :sort   'sort-command})


(defn parse-params
  "Return a restructuring of params, which is of form:
     [arg* (& more)?]
  into
     [(arg1 arg2 ..) more]"
  [params]
  (let [[args rest] (split-with #(not= % '&) params)]
    [args (last rest)]))

(defmacro defcommand
  "Define a function for Redis command name with parameters
  params. Type is one of :inline, :bulk or :sort, which determines how
  the command string is constructued."
  ([name params type] `(defcommand ~name ~params ~type (fn [reply#] reply#)))
  ([name params type reply-fn] `(~name ~params ~type ~reply-fn)
     (do
       (let [command (uppercase (str name))
             command-fn (type command-fns)
             [command-params
              command-params-rest] (parse-params params)]
         `(defn ~name
            ~params
            (let [request# (apply ~command-fn
                                  ~command
                                  ~@command-params
                                  ~command-params-rest)]
              (send-command request#)
              (~reply-fn (read-reply)))))
       
       )))


(defmacro defcommands
  [& command-defs]
  `(do ~@(map (fn [command-def]
              `(defcommand ~@command-def)) command-defs)))



