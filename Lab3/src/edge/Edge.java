package edge;

public class Edge<E> {
    // E是边的属性泛型, L是顶点的属性泛型
    private String destinationVertex;
    private E edgeValue;

    public Edge(String destinationVertex, E edgeValue) {
        this.destinationVertex = destinationVertex;
        this.edgeValue = edgeValue;
    }

    public String getDestinationVertex() {
        return destinationVertex;
    }

    public E getEdgeValue() {
        return edgeValue;
    }
}
