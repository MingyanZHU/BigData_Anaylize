import matplotlib.pyplot as plt
import pickle

with open("usdata.pickle", "rb") as usdata:
    data = pickle.load(usdata)
    print(data[:, 0])
    plt.plot(data[:, 0], data[:, 1])
    plt.show()
