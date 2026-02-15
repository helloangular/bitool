(ns bitool.utilities
  (:import (javax.crypto Cipher KeyGenerator )
           (javax.crypto.spec IvParameterSpec SecretKeySpec)
           (java.util Base64)))

(defn generate-key []
  (let [keygen (KeyGenerator/getInstance "AES")]
    (.init keygen 128)
    (let [secret-key (.generateKey keygen)]
      (SecretKeySpec. (.getEncoded secret-key) "AES"))))

(defn generate-iv []
  (let [iv-bytes (byte-array 16)]
    (.nextBytes (java.security.SecureRandom.) iv-bytes)
    (IvParameterSpec. iv-bytes)))

(defn pad-to-block-size [data block-size]
  (let [pad-length (mod (- block-size (mod (count data) block-size)) block-size)]
    (str data (apply str (repeat pad-length " ")))))

(defn encrypt [key iv data]
  (let [cipher (Cipher/getInstance "AES/CBC/PKCS5Padding")
        padded-data (pad-to-block-size data 16)]
    (.init cipher Cipher/ENCRYPT_MODE key iv)
    (.doFinal cipher (.getBytes padded-data))))

(defn base52-encode [data length alphabet]
  (let [base 52]
    (apply str
           (map #(nth alphabet (mod % base))
                (take length (concat (map byte data) (repeat 0)))))))

(def A "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz" )
(def D  "012345678901234567890123456789012345678901234567890123456789" )
(def N "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" )
(def X "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~" )

(def secret-key (generate-key))
(def iv (generate-iv))

(defn getMaskType [mask] 
      (case mask
          "A" A
    	  "D" D
    	  "N" N
          "_" "_"
    	  X))

(defn encrypted-text [mask source] 
    (let [masktype (getMaskType mask)]
       (if (= masktype "_") source (base52-encode (encrypt secret-key iv source) (count source) masktype))))

(defn partition-repeating [s]
  (loop [chars (seq s)
         current (first chars)
         acc []
         result []]
    (if (empty? chars)
      (conj result (apply str acc))
      (let [next-char (first chars)
            remaining-chars (rest chars)]
        (if (= next-char current)
          (recur remaining-chars current (conj acc next-char) result)
          (recur remaining-chars next-char [next-char] (conj result (apply str acc))))))))

(defn partition-by-lengths [lengths s]
  (loop [remaining-lengths lengths
         remaining-string s
         result []]
    (if (empty? remaining-lengths)
      (conj result remaining-string)
      (let [length (first remaining-lengths)
            partition (subs remaining-string 0 (min length (count remaining-string)))
            new-remaining-string (subs remaining-string (min length (count remaining-string)))]
        (recur (rest remaining-lengths) new-remaining-string (conj result partition))))))

(defn partition-second-by-first [s1 s2]
  (let [partitions (partition-repeating s1)
        _ (println (str "partiotions " partitions))
        lengths (map count partitions)
        _ (println (str "legths " (vec lengths)))]
    (partition-by-lengths lengths s2)))

(defn map-merge [f list1 list2]
  (let [len1 (count list1)
        _ (println len1)
        len2 (count list2) 
        _ (println len2) ]
        (if (<= len2 len1)
            (do 
                (println "Inside Do 1")
                (map (fn [[a b]] (f a b)) list1 list2)
                (println "Done 1"))
            (let [ paired (map vector list1 list2)
                   _ (println (str "Paired : " paired))
                   extra  (drop len1 list2) 
                   _ (println (str "Extra : " extra)) ]
                 (concat (map (fn [[a b]] (f a b)) paired) extra)))))

(defn mask [s1 s2] 
    (let [masks (map str (map first (partition-repeating s1)))
          _ (println masks)
          split_s2 (partition-second-by-first s1 s2)
          _ (println split_s2)]
          (reduce str (map-merge encrypted-text masks split_s2 ))))
           






