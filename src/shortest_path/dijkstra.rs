use std::collections::BinaryHeap;

use crate::{
    col::{HashSet, set_with_capacity},
    graph::{EdgeIdx, EdgePayload, Graph, NodeIdx, NodeType, StationIdx},
    primitives::Time,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueItem {
    node_id: NodeIdx,
    arrival: Time,
}
impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .arrival
            .cmp(&self.arrival)
            .then_with(|| self.node_id.0.cmp(&other.node_id.0))
    }
}

pub struct DijkstraResult {
    pub reachable: HashSet<NodeIdx>,
    pub destination: NodeIdx,
}
pub fn dijkstra(
    graph: &Graph,
    source_id: NodeIdx,
    destination: StationIdx,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
) -> DijkstraResult {
    let source = graph.node(source_id);

    let mut reachable: HashSet<NodeIdx> = set_with_capacity(graph.num_nodes());
    reachable.insert(source_id);

    let mut queue: BinaryHeap<QueueItem> = BinaryHeap::new();
    queue.push(QueueItem {
        node_id: source_id,
        arrival: source.time,
    });

    while let Some(QueueItem {
        node_id,
        arrival: _,
    }) = queue.pop()
    {
        let node = graph.node(node_id);
        if node.node_type == NodeType::Wait(destination) {
            return DijkstraResult {
                reachable,
                destination: node_id,
            };
        }

        for &edge_idx in node.outgoing.iter() {
            let edge = graph.edge(edge_idx);
            if !edge_okay(edge_idx, edge) {
                continue;
            }
            let to_node_id = edge.to;
            let to_node = graph.node(to_node_id);
            if reachable.insert(to_node_id) {
                queue.push(QueueItem {
                    node_id: to_node_id,
                    arrival: to_node.time,
                });
            }
        }
    }

    panic!(
        "Could not find destination {:?} from node {:?}",
        destination, source_id
    );
}
