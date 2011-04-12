(ns diffusion.rdf-test
    (:use diffusion.rdf
          clojure.test
          midje.sweet)
    (:require [plaza.rdf.core :as prc]
              [plaza.rdf.sparql :as prs]))

(facts "We can detect which type a node has"
    (user? (make-user .any.)) => truthy
    (user? (make-item .any.)) => falsey
    (user? (make-tag  .any.)) => falsey
    (item? (make-user .any.)) => falsey
    (item? (make-item .any.)) => truthy
    (item? (make-tag  .any.)) => falsey
    (tag?  (make-user .any.)) => falsey
    (tag?  (make-item .any.)) => falsey
    (tag?  (make-tag  .any.)) => truthy)
    
(fact "A new create graph has no nodes"
    (prc/model-to-triples (new-graph)) => (just []))
            
(defn contains-only-one-with? [type elem]
    (fn [model]
        (let [[only :as result] (all-of-type model type)]
            (is (= (count result) 1))
            (is (= only elem)))))
            
(def all-reviews
    (prs/defquery
        (prs/query-set-type :select)
        (prs/query-set-vars [:?user :?item])
        (prs/query-set-pattern
            (prs/make-pattern [[:?user prc/rdf:type [:diffusion :User]]
                                   [:?item prc/rdf:type [:diffusion :Item]]
                                   [:?user [:diffusion :has-reviewed] :?item]]))))

(def all-tagged-items
   (prs/defquery
       (prs/query-set-type :select)
       (prs/query-set-vars [:?item :?tag])
       (prs/query-set-pattern
           (prs/make-pattern [[:?item prc/rdf:type [:diffusion :Item]]
                                  [:?tag prc/rdf:type [:diffusion :Tag]]
                                  [:?item [:diffusion :has-tag] :?tag]]))))

(defn contains-only-one-review-with [user item]
    (fn [model]
        (let [[only :as result] (prs/model-query model all-reviews)]
            (is (= (count result) 1))
            (is (= (:?user only) (make-user user)))
            (is (= (:?item only) (make-item item))))))
            
(defn contains-only-one-tagged-item-with [item tag]
    (fn [model]
        (let [[only :as result] (prs/model-query model all-tagged-items)]
            (is (= (count result) 1))
            (is (= (:?item only) (make-item item)))
            (is (= (:?tag only) (make-tag tag))))))
            
(defn also [& checkers]
    (fn [result]
        (every? (fn [checker] (checker result)) checkers)))

(fact "Adding a user to an empty graph creates a graph with only this user" 
    (add-user (new-graph) "user")
    => (contains-only-one-with? :User "user"))

(fact "Adding an item to an empty graph creates a graph with only this item"
    (add-item (new-graph) "item")
    => (contains-only-one-with? :Item "item"))

(fact "Adding a tag to an empty graph creates a graph with only this tag"
    (add-tag (new-graph) "tag")
    => (contains-only-one-with? :Tag "tag"))

(facts "Adding a review to an empty graph boths adds the two nodes and an edge"
    (add-review (new-graph) "user" "item") 
    => (also (contains-only-one-with? :User "user")
             (contains-only-one-with? :Item "item")
             (contains-only-one-review-with "user" "item")))

 (facts "Tagging an item on an empty graph both adds the two nodes and an edge"
     (tag-item (new-graph) "item" "tag") 
     => (also (contains-only-one-with? :Item "item")
              (contains-only-one-with? :Tag  "tag")
              (contains-only-one-tagged-item-with "item" "tag")))
              
(def graph1 (-> (new-graph)
              (add-review "u1" "i1")
              (add-review "u1" "i2")
              (add-review "u2" "i2")
              (tag-item "i1" "t1")
              (tag-item "i1" "t2")
              (tag-item "i2" "t1")))

(facts "We can count the reviews made by a user"
  (count-reviews-of-user graph1 "u1") => 2
  (count-reviews-of-user graph1 "u2") => 1)
  
(facts "We can count how many reviewers an item has had"
  (count-reviewers-of-item graph1 "i1") => 1
  (count-reviewers-of-item graph1 "i2") => 2)
  
(facts "We can count how many tags an item has"
  (count-tags-of-item graph1 "i1") => 2
  (count-tags-of-item graph1 "i2") => 1)

(facts "We can count how many items are tagged with a tag"
  (count-items-with-tag graph1 "t1") => 2
  (count-items-with-tag graph1 "t2") => 1)
  
(facts "We can count all users, items and tags"
  (all-users graph1) => (just "u1" "u2" :in-any-order)
  (all-items graph1) => (just "i1" "i2" :in-any-order)
  (all-tags  graph1) => (just "t1" "t2" :in-any-order))

(facts "We know who reviewed an item"
  (reviewed-by graph1 "i1") => (just "u1")
  (reviewed-by graph1 "i2") => (just "u1" "u2" :in-any-order))

(facts "We know what an user has reviewed"
  (reviews-of graph1 "u1") => (just "i1" "i2" :in-any-order)
  (reviews-of graph1 "u2") => (just "i2"))

  (facts "We know what an user has reviewed"
      (tagged-with graph1 "t1") => (just "i1" "i2" :in-any-order)
      (tagged-with graph1 "t2") => (just "i1"))

(facts "We know what an user has reviewed"
  (tags-of graph1 "i1") => (just "t1" "t2" :in-any-order)
  (tags-of graph1 "i2") => (just "t1"))

(facts "We know the items new for a user (those not reviewd bt him)"
  (new-items-for graph1 "u1") => (just [])
  (new-items-for graph1 "u2") => (just "i1"))
