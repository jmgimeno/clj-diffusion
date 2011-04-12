(ns diffusion.graph-test
    (:use diffusion.graph
          midje.sweet)
    (:require [loom.graph :as lg])
    (:import [diffusion.graph Node]))
    
(facts "Nodes are created correctly"
    (make-user .user.) => (Node. .user. :User)
    (make-item .item.) => (Node. .item. :Item)
    (make-tag  .tag.)  => (Node. .tag.  :Tag))
    
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
    (lg/nodes (new-graph)) => (just []))
          
(fact "Adding a user to an empty graph creates a graph with only this user" 
    (lg/nodes (add-user (new-graph) .user.)) => (just (make-user .user.)))

(fact "Adding an item to an empty graph creates a graph with only this item"
    (lg/nodes (add-item (new-graph) .item.)) => (just (make-item .item.)))
    
(fact "Adding a tag to an empty graph creates a graph with only this tag"
    (lg/nodes (add-tag (new-graph) .tag.)) => (just (make-tag .tag.)))

(facts "Adding a review to an empty graph boths adds the two nodes and an edge"
    (lg/nodes (add-review (new-graph) .user. .item.)) => (just (make-user .user.) (make-item .item.)
                                                            :in-any-order)
    (lg/edges (add-review (new-graph) .user. .item.)) => (just [(make-user .user.) (make-item .item.)]
                                                            [(make-item .item.) (make-user .user.)]
                                                            :in-any-order))

(facts "Tagging an item on an empty graph both adds the two nodes and an edge"
    (lg/nodes (tag-item (new-graph) .item. .tag.)) => (just (make-tag .tag.) (make-item .item.)
                                                         :in-any-order)
    (lg/edges (tag-item (new-graph) .item. .tag.)) => (just [(make-tag .tag.) (make-item .item.)]
                                                         [(make-item .item.) (make-tag .tag.)]
                                                         :in-any-order))

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

(def graph2 (-> (new-graph)
                (add-review "u1" "i1")
                (add-review "u1" "i3")
                (add-review "u1" "i5")
                (add-review "u2" "i2")
                (add-review "u2" "i3")
                (add-review "u2" "i4")
                (add-review "u3" "i1")
                (add-review "u3" "i2")
                (add-review "u3" "i4")
                (add-review "u3" "i5")
                (tag-item "i1" "t1")
                (tag-item "i1" "t2")
                (tag-item "i1" "t4")
                (tag-item "i2" "t1")
                (tag-item "i2" "t3")
                (tag-item "i3" "t2")
                (tag-item "i3" "t4")
                (tag-item "i4" "t3")
                (tag-item "i4" "t4")
                (tag-item "i5" "t3")))

(facts "The initial activations for a user are 1.0 for the items reviewed and zero otherwise"
    (initial-activations graph2 "u1") => {"i1" 1.0 "i2" 0.0 "i3" 1.0 "i4" 0.0 "i5" 1.0})

(facts "We can count users->items, items->tags, items->users, tags->items"
    (counts-users-to-items graph2) => {"u1" 3 "u2" 3 "u3" 4}
    (counts-items-to-users graph2) => {"i1" 2 "i2" 2 "i3" 2 "i4" 2 "i5" 2}
    (counts-items-to-tags graph2)  => {"i1" 3 "i2" 2 "i3" 2 "i4" 2 "i5" 1}
    (counts-tags-to-items graph2)  => {"t1" 2 "t2" 2 "t3" 3 "t4" 3})

(facts "We can compute the diffusions users-items and items-tags"
    (diffusion-users-items-for-user graph2 "u1") => (just ["i2" (roughly (/ 5.0 12.0))] 
                                                          ["i4" (roughly (/ 5.0 12.0))]
                                                          :in-any-order)
                                                          
    (diffusions-users-items graph2) => (contains {"u1" (just ["i2" (roughly (/ 5.0 12.0))] 
                                                             ["i4" (roughly (/ 5.0 12.0))]
                                                             :in-any-order)})

    (diffusion-items-tags-for-user graph2 "u1") => (just ["i2" (roughly (/  1.0  2.0))] 
                                                         ["i4" (roughly (/ 11.0 18.0))]
                                                         :in-any-order)
                                                    
    (diffusions-items-tags graph2) => (contains {"u1" (just ["i2" (roughly (/  1.0  2.0))] 
                                                            ["i4" (roughly (/ 11.0 18.0))]
                                                            :in-any-order)}))


                                                         
            

