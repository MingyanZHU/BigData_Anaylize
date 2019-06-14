package edge;

import vertex.Vertex;

public class Edge<L, E> {
    // E是边的属性泛型, L是顶点的属性泛型
    private Vertex<L> destinationVertex;
    private E edgeValue;

    public Edge(Vertex<L> destinationVertex, E edgeValue) {
        this.destinationVertex = destinationVertex;
        this.edgeValue = edgeValue;
    }

    public Vertex<L> getDestinationVertex() {
        return destinationVertex;
    }

    public E getEdgeValue() {
        return edgeValue;
    }
}
