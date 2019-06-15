package utils;

public class Statistician {
    private double timeUsed;
    private int vertexNumber;
    private int edgeNumber;
    private int communicationNumber;
    private int superStep;

    public Statistician(int vertexNumber, int edgeNumber) {
        this.vertexNumber = vertexNumber;
        this.edgeNumber = edgeNumber;
        this.communicationNumber = 0;
        this.superStep = 0;
        this.timeUsed = 0;
    }

    public double getTimeUsed() {
        return timeUsed;
    }

    public int getVertexNumber() {
        return vertexNumber;
    }

    public int getEdgeNumber() {
        return edgeNumber;
    }

    public int getCommunicationNumber() {
        return communicationNumber;
    }

    public void setTimeUsed(double timeUsed) {
        this.timeUsed = timeUsed;
    }

    public void setVertexNumber(int vertexNumber) {
        this.vertexNumber = vertexNumber;
    }

    public void setEdgeNumber(int edgeNumber) {
        this.edgeNumber = edgeNumber;
    }

    public void setCommunicationNumber(int communicationNumber) {
        this.communicationNumber = communicationNumber;
    }

    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }

    public int getSuperStep() {
        return superStep;
    }

    @Override
    public String toString() {
        return "timeUsed=" + timeUsed +
                ", vertexNumber=" + vertexNumber +
                ", edgeNumber=" + edgeNumber +
                ", communicationNumber=" + communicationNumber +
                ", superStep=" + superStep;
    }
}
