#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(non_snake_case)]

////////////////////////////////////////////////////////////////////////////////
// Imports
use uuid::Uuid;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

////////////////////////////////////////////////////////////////////////////////
// Graph Abstract Data Type Starts Here


#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EdgeId {
    pub id: String,
    typ: String,
}
impl EdgeId {
    fn new(typ: String) -> Self {
        EdgeId {
            id : Uuid::new_v4().to_string(),
            typ,
        }
    }

    // UUID identiers can get pretty verbose to weidld.
    // the alias function returns a truncated version of the eight first characters. 
    pub fn alias(&self) -> String {
        self.id.clone().chars().take(8).collect()
    }
}

/// A NodeId represents a unique identifier that refers to a node element in the graph.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct NodeId {
    pub id: String
}
impl NodeId {
    fn new() -> Self {
        NodeId {
            id : Uuid::new_v4().to_string(),
        }
    }

    fn get(&self) -> &String {
        &self.id
    }

    // UUID identiers can get pretty verbose to weild.
    // The alias function returns a truncated version of the eight first characters. 
    pub fn alias(&self) -> String {
        self.id.clone().chars().take(8).collect()
    }
}

/// The graph is represented as an adjacency list.
#[derive(Debug, Clone)]
pub struct Graph {
    // Graph related fields.
    nodes: HashMap<NodeId, HashSet<EdgeId>>,
    adjacencylist: HashMap<NodeId, Vec<(NodeId, EdgeId)>>,
    // MCC related fields.
    next_xid: u32,
    active_xids: BTreeSet<u32>,
    modifications: BTreeSet<BTreeMap<String, u32>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            adjacencylist: HashMap::new(),
            next_xid : 0,
            active_xids : BTreeSet::new(),
            modifications : BTreeSet::new(),
        }
    }
}    


impl Graph {
    // TODO: handle case where the passed TransactionId is Void.
    pub fn add_node(&mut self, t: &TransactionId) -> NodeId {
        let minted_node_id = NodeId::new();
        let node_id = minted_node_id.clone();
        self.nodes.entry(minted_node_id).or_insert_with(HashSet::new);

        node_id
    }
    
    pub fn add_edge(&mut self,  t: &TransactionId, from: &NodeId, to: &NodeId, edge_type: String) {
        let minted_edge = EdgeId::new(edge_type);
        self.set_directed_edge(from, to, minted_edge.clone());
        self.set_directed_edge(to, from, minted_edge);
    }

    pub fn set_directed_edge(&mut self, from: &NodeId, to: &NodeId, edge: EdgeId) {
        let src_edge_dst = self.adjacencylist.entry(from.clone()).or_default();
        src_edge_dst.push((to.clone(), edge));
    }

    pub fn get_nodes(&self, t: &TransactionId, origin: &NodeId, search_path: Vec<String>) -> Vec<NodeId> {
        let type_path = TypePath { 
            graph: self, 
            current_node: Some(origin.clone()),
            type_list: search_path,
            path_list: VecDeque::new(),
        };

        let path: Vec<NodeId> = type_path.into_iter().collect();
        path
    }
    
    pub fn start_transaction(&mut self) -> TransactionId {        
        self.next_xid += 1;
        self.active_xids.insert(self.next_xid);
        
        TransactionId::new(self.next_xid)
    }

    pub fn set_tx_expiration_value(&mut self, pos: u32, n:u32) {
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

pub struct TypePath<'graph> {
    graph: &'graph Graph,
    current_node: Option<NodeId>,
    type_list: Vec<String>,
    path_list: VecDeque<NodeId>,
}

impl<'graph> Iterator for TypePath<'graph> {
    type Item = NodeId;

    fn next(&mut self) -> Option<NodeId> {
        if let Some(node) = self.current_node.take() {
            let edge_list = self.graph.adjacencylist.get(&node)?;

            if let Some(current_type) = self.type_list.pop() { // .unwrap()
                if let Some((ni,ei)) = edge_list.iter().next() {
                    if ei.typ == current_type {
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



////////////////////////////////////////////////////////////////////////////////
// MVCC Start
#[derive(Debug, Clone)]
pub struct TransactionId {
    pub xid:u32,
    rollback_actions: BTreeSet<BTreeMap<String, u32>>,
}

impl TransactionId {
    pub fn new(xid:u32) -> Self {
        TransactionId {
            xid,
            rollback_actions : BTreeSet::new(),
        }
    }
    
    pub fn add_modification(&mut self, graph: &mut Graph, modification: &mut BTreeMap<String, u32>) {
        modification.insert("created_xid".to_string(), self.xid);
        modification.insert("expired_xid".to_string(), 0);

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
                    modification.clone().insert("expired_xid".to_string(), self.xid);
                    let mut new_rec: BTreeMap<String,u32> = BTreeMap::new();
                    new_rec.insert("add".to_string(), i as u32);
                    self.rollback_actions.insert(new_rec);
                }
            }
        }
    }
    
    fn graph_element_visible(&self, graph: &Graph, modification: &BTreeMap<String, u32>) -> bool {
        if graph.active_xids.contains(modification.get(&"created_xid".to_string()).unwrap())
        && modification.get(&"created_xid".to_string()) != Some(&self.xid) {
            return false;
        }
    
        if (modification.get(&"expired_xid".to_string()) != Some(&0)) 
        && ((!graph.active_xids.contains(modification.get(&"expired_xid".to_string()).unwrap())) 
        || (modification.get(&"created_xid".to_string()) == Some(&self.xid))) {
            return false;
        }

        true
    }
    
    fn graph_element_locked(&self, graph: &Graph, modification: &BTreeMap<String, u32>) -> bool {
        (modification.get(&"expired_xid".to_string()).unwrap() != &0) 
        &&
        (graph.active_xids.contains(modification.get(&"expired_xid".to_string()).unwrap()))
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
        graph.active_xids.remove(&self.xid);
    }

    fn rollback(&mut self, g: &mut Graph) {
        for action in self.rollback_actions.iter().rev() {
            let mut map = action.iter();
            let (action_type, action_position) = map.next().unwrap().first();
            let pos:u32 = action_position;

            if action_type == &"add".to_string() {
                g.set_tx_expiration_value(pos, 0);
            } else if action_type == &"delete".to_string() {
                g.set_tx_expiration_value(pos, self.xid);
            }
        } 

        g.active_xids.remove(&self.xid);
    }
}
 


////////////////////////////////////////////////////////////////////////////////
// Unit Tests Start
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_case_01() {
        // "Test case 01: Add 2 nodes connected by an edge and follow that node"
        let mut g = Graph::new();  
        let t: TransactionId = g.start_transaction();
        let n1: NodeId = g.add_node(&t);
        let n2: NodeId = g.add_node(&t);
        g.add_edge(&t, &n1, &n2, "red".to_string());
        
        // We match the result of g.get_nodes against the Unique Universal Identifier.
        assert_eq!(n2.alias(), g.get_nodes(&t, &n1, vec!(String::from("red"))).first().unwrap().alias());
        assert_eq!(0, g.get_nodes(&t, &n1, vec!(String::from("blue"))).len());
        assert_eq!(n1.alias(), g.get_nodes(&t, &n2, vec!(String::from("red"))).first().unwrap().alias());
        assert_eq!(0, g.get_nodes(&t, &n2, vec!(String::from("blue"))).len());
    }
}