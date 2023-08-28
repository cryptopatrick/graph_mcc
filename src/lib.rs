#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(non_snake_case)]

use uuid::Uuid;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Edge {
    pub id: String,
    edgetype: String,
}
impl Edge {
    fn new(typ: String) -> Self {
        Edge {
            id : Uuid::new_v4().to_string(),
            edgetype: typ,
        }
    }

    pub fn alias(&self) -> String {
        self.id.clone().chars().take(8).collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Node {
    pub id: String
}
impl Node {
    fn new() -> Self {
        Node {
            id : Uuid::new_v4().to_string(),
        }
    }

    fn get(&self) -> &String {
        &self.id
    }

    pub fn alias(&self) -> String {
        self.id.clone().chars().take(8).collect()
    }
}

#[derive(Debug, Clone)]
pub struct Graph {
    nodes: HashMap<Node, HashSet<Edge>>,
    adjacencylist: HashMap<Node, Vec<(Node, Edge)>>,
    next_transaction_id: u32,
    active_transaction_ids: BTreeSet<u32>,
    modifications: BTreeSet<BTreeMap<String, u32>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            adjacencylist: HashMap::new(),
            next_transaction_id : 0,
            active_transaction_ids : BTreeSet::new(),
            modifications : BTreeSet::new(),
        }
    }
}    

impl Graph {
    pub fn add_node(&mut self, t: &TransactionId) -> Node {
        let minted_node = Node::new();
        let node = minted_node.clone();
        self.nodes.entry(minted_node).or_insert_with(HashSet::new);

        node
    }
    
    pub fn add_edge(&mut self,  t: &TransactionId, from: &Node, to: &Node, edge_type: String) {
        let minted_edge = Edge::new(edge_type);
        self.set_directed_edge(from, to, minted_edge.clone());
        self.set_directed_edge(to, from, minted_edge);
    }

    // Utility function to create bidirectional edges so the graph is undirected.
    pub fn set_directed_edge(&mut self, from: &Node, to: &Node, edge: Edge) {
        let src_edge_dst = self.adjacencylist
            .entry(from.clone()).or_default();
        src_edge_dst.push((to.clone(), edge));
    }

    pub fn get_nodes(&self, t: &TransactionId, origin: &Node, search_path: Vec<String>) -> Vec<Node> {
        let type_path = TypePath { 
            graph: self, 
            current_node: Some(origin.clone()),
            // TODO: Improve naming of type_list and path_list variables.
            type_list: search_path,
            path_list: VecDeque::new(),
        };
        
        let path: Vec<Node> = type_path.into_iter().collect();
        path
    }
    
    pub fn start_transaction(&mut self) -> TransactionId {        
        self.next_transaction_id += 1;
        self.active_transaction_ids.insert(self.next_transaction_id);
        
        TransactionId::new(self.next_transaction_id)
    }
    
    // FIX: Error mutating a borrow.
    pub fn set_transaction_expiration(&mut self, pos: u32, n:u32) {
        self.modifications.iter().skip(pos as usize - 1)
            .next()
            .unwrap()
            .insert("expired".to_string(), n);
    } 
    
}

impl Default for Graph {
    fn default() -> Self {
        Self::new()
    }
}

/// TypePath represents a traversal of the graph based on the sequence of types 
/// leading from a starting node, through all adjancent nodes connected via edges
/// matching the sequence of types in the type path.
// TODO: write an example of using TypePath.
pub struct TypePath<'graph> {
    graph: &'graph Graph,
    current_node: Option<Node>,
    // TODO: Improve naming of type_list and path_list variables.
    type_list: Vec<String>,
    path_list: VecDeque<Node>,
}

impl<'graph> Iterator for TypePath<'graph> {
    type Item = Node;

    fn next(&mut self) -> Option<Node> {
        if let Some(node) = self.current_node.take() {
            let edge_list = self.graph.adjacencylist.get(&node)?;

            if let Some(current_type) = self.type_list.pop() { // .unwrap()
                if let Some((ni,ei)) = edge_list.iter().next() {
                    if ei.edgetype == current_type {
                        self.path_list.push_back(ni.clone());
                        self.current_node = Some(ni.clone());
                        return Some(ni.clone());
                    }
                }
            }
        }
        
        self.current_node = None;
        None
    }
}


#[derive(Debug, Clone)]
pub struct TransactionId {
    pub id:u32,
    rollback_actions: BTreeSet<BTreeMap<String, u32>>,
}

impl TransactionId {
    pub fn new(xid:u32) -> Self {
        TransactionId {
            id: xid,
            rollback_actions : BTreeSet::new(),
        }
    }
    
    pub fn add_modification(&mut self, graph: &mut Graph, modification: &mut BTreeMap<String, u32>) {
        modification.insert("transaction_creation_id".to_string(), self.id);
        modification.insert("transaction_expiration_id".to_string(), 0);

        let mut action:BTreeMap<String, u32> = BTreeMap::new();
        action.insert("delete".to_string(), graph.modifications.len() as u32);
        self.rollback_actions.insert(action);
        
        graph.modifications.insert(modification.clone());
    }
    
    fn delete_modification(&mut self, id: u32, graph: &mut Graph) {
        for (i, modification) in graph.modifications.iter().enumerate() {
            if self.graph_element_visible(graph, modification) && modification.get(&"id".to_string()).unwrap() == &id {
                if self.graph_element_locked(graph, modification) {
                    panic!("Graph element is locked by another transaction.");
                } else {
                    modification.clone().insert("transaction_expiration_id".to_string(), self.id);
                    let mut new_rec: BTreeMap<String,u32> = BTreeMap::new();
                    new_rec.insert("add".to_string(), i as u32);
                    self.rollback_actions.insert(new_rec);
                }
            }
        }
    }
    
    fn graph_element_visible(&self, graph: &Graph, modification: &BTreeMap<String, u32>) -> bool {
        if graph.active_transaction_ids.contains(modification.get(&"transaction_creation_id".to_string()).unwrap())
        && modification.get(&"transaction_creation_id".to_string()) != Some(&self.id) {
            return false;
        }
    
        if (modification.get(&"transaction_expiration_id".to_string()) != Some(&0)) 
        && ((!graph.active_transaction_ids.contains(modification.get(&"transaction_expiration_id".to_string()).unwrap())) 
        || (modification.get(&"transaction_creation_id".to_string()) == Some(&self.id))) {
            return false;
        }

        true
    }
    
    fn graph_element_locked(&self, graph: &Graph, modification: &BTreeMap<String, u32>) -> bool {
        (modification.get(&"transaction_expiration_id".to_string()).unwrap() != &0) 
        &&
        (graph.active_transaction_ids.contains(modification.get(&"transaction_expiration_id".to_string()).unwrap()))
    }

    fn update_modification(&mut self, id:u32, num:String, graph: &mut Graph) {
        self.delete_modification(id, graph);
        let mut new_modification_version: BTreeMap<String,u32> = BTreeMap::new();
        new_modification_version.insert("id".to_string(), id);
        self.add_modification(graph, &mut new_modification_version);
    }

    fn create_snapshot(&self, graph: &mut Graph) -> BTreeSet<BTreeMap<String, u32>> {
        let mut visible_modifications = BTreeSet::new();
    
        for modification in graph.modifications.iter() {
            if self.graph_element_visible(graph, modification) {
                visible_modifications.insert(modification.clone());
            }
        }

        visible_modifications
    }

    fn commit(&self, graph: &mut Graph) {
        graph.active_transaction_ids.remove(&self.id);
    }

    fn rollback(&mut self, g: &mut Graph) {
        for action in self.rollback_actions.iter().rev() {
            let mut map = action.iter();

            let item = map.next().unwrap();
            let (action_type, action_position) = item;
            let pos:u32 = action_position.clone();
            
            if action_type == &"add".to_string() {                
                g.set_transaction_expiration(pos, 0);
            } else if action_type == &"delete".to_string() {
                 g.set_transaction_expiration(pos, self.id);
            }
        } 

        g.active_transaction_ids.remove(&self.id);
    }
}
 


#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_add_node() {
        let mut graph = Graph::new();
        let tx = graph.start_transaction();
        let node_id = graph.add_node(&tx);

        assert!(graph.nodes.contains_key(&node_id));
    }

    #[test]
    fn test_add_edge() {
        let mut graph = Graph::new();
        let tx = graph.start_transaction();
        let node1 = graph.add_node(&tx);
        let node2 = graph.add_node(&tx);

        graph.add_edge(&tx, &node1, &node2, "edge_type".to_string());

        let adjacency_list = graph.adjacencylist.get(&node1).unwrap();
        assert_eq!(adjacency_list.len(), 1);
        assert_eq!(adjacency_list[0].0, node2);
    }

    #[test]
    fn test_get_nodes() {
        let mut graph = Graph::new();
        let tx = graph.start_transaction();
        let node1 = graph.add_node(&tx);
        let node2 = graph.add_node(&tx);
        graph.add_edge(&tx, &node1, &node2, "edge_type".to_string());

        let search_path = vec!["edge_type".to_string()];
        let nodes = graph.get_nodes(&tx, &node1, search_path);

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], node2);
    }

    #[test]
    fn test_transaction_commit() {
        let mut graph = Graph::new();
        let tx = graph.start_transaction();

        tx.commit(&mut graph);

        assert!(!graph.active_transaction_ids.contains(&tx.id));
    }

    #[test]
    fn test_transaction_rollback() {
        let mut graph = Graph::new();
        let mut tx = graph.start_transaction();
        let node1 = graph.add_node(&tx);
        let mut modification = BTreeMap::new();
        modification.insert("id".to_string(), 1);

        tx.add_modification(&mut graph, &mut modification);
        tx.rollback(&mut graph);

        assert_eq!(graph.modifications.len(), 0);
    }
}