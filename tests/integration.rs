use graph_mcc::*;

#[test]
fn test_case_01() {
    // "Test case 01: Add 2 nodes connected by an edge and follow that node"
    let mut g = Graph::new();  
    let t: TransactionId = g.start_transaction();
    let n1: Node = g.add_node(&t);
    let n2: Node = g.add_node(&t);
    g.add_edge(&t, &n1, &n2, "red".to_string());
    
    // We match the result of g.get_nodes against the Unique Universal Identifier.
    assert_eq!(n2.alias(), g.get_nodes(&t, &n1, vec!(String::from("red"))).first().unwrap().alias());
    assert_eq!(0, g.get_nodes(&t, &n1, vec!(String::from("blue"))).len());
    assert_eq!(n1.alias(), g.get_nodes(&t, &n2, vec!(String::from("red"))).first().unwrap().alias());
    assert_eq!(0, g.get_nodes(&t, &n2, vec!(String::from("blue"))).len());
}