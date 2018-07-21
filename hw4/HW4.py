#import matplotlib.pyplot as plt
import numpy as np
from hierarchical import hierarchical_clustering
#from matplotlib.pyplot import savefig
from sklearn.neighbors import NearestNeighbors
from scipy.spatial import distance
import sys

sample_data_path = sys.argv[1]
full_data_path = sys.argv[2]
K = int(sys.argv[3])
N = int(sys.argv[4])
P = float(sys.argv[5])
output_file = sys.argv[6]


def get_spaced_colors(n):
    max_value = 16581375  # 255**3
    interval = int(max_value / n)
    colors = [hex(I)[2:].zfill(6) for I in range(0, max_value, interval)]

    return [(int(i[:2], 16), int(i[2:4], 16), int(i[4:], 16)) for i in colors]


# def save_fig(data, initial_labels, K):
#     for i in range(K):
#         plt.plot(data[initial_labels == i][:, 0], data[initial_labels == i][:, 1],
#                  color=(np.random.uniform(0, 1), np.random.uniform(0, 1), np.random.uniform(0, 1)), marker='o',
#                  markersize=4, linestyle=' ')
#     savefig('./Initial_Clusters')
#     plt.close()
#     return


def read_data(filename):
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    data = [[float(i) for i in x.split(',')] for x in content]
    data = np.array(data)
    data = data.reshape([-1, 2])

    return data


def get_representatives(data, initial_labels, N):
    clusters_representatives = []
    for i in range(K):
        representatives = []
        cluster = data[initial_labels == i]
        min_x_indices = np.argmin(cluster[:, 0], axis=0)
        if min_x_indices.shape == ():
            first_representative = np.array([[cluster[min_x_indices, 0], cluster[min_x_indices, 1]]])
            cluster = np.delete(cluster, [min_x_indices], axis=0)
        else:
            relative_index = np.argmin(cluster[min_x_indices], axis=1)
            first_representative = np.array(
                [cluster[min_x_indices][relative_index[0], 0], cluster[min_x_indices][relative_index[0], 1]])
            cluster = np.delete(cluster, [min_x_indices, relative_index[0]], axis=0)
        representatives.extend(first_representative)

        for j in range(1, N):
            dist = distance.cdist(np.array(representatives).reshape([-1, 2]), cluster)
            dist = np.sum(dist, axis=0)
            representatives.extend([cluster[np.argmax(dist)]])
            cluster = np.delete(cluster, np.argmax(dist), axis=0)
        representatives = np.array(representatives)
        clusters_representatives.extend([representatives])

    clusters_representatives = np.array(clusters_representatives)

    # for i in range(K):
    #     plt.plot(clusters_representatives[i][:, 0], clusters_representatives[i][:, 1],
    #              color=(np.random.uniform(0, 1), np.random.uniform(0, 1), np.random.uniform(0, 1)), marker='o',
    #              markersize=7, linestyle=' ')
    # plt.plot(data[:, 0], data[:, 1],
    #          color=(np.random.uniform(0, 1), np.random.uniform(0, 1), np.random.uniform(0, 1)), marker='o',
    #          markersize=2, linestyle=' ')
    #savefig('Representatives')
    #plt.close()
    # print '########Representatives Before Movements#############'
    # print clusters_representatives
    return clusters_representatives


def get_new_representatives(representatives, P):
    new_representatives = []
    for i in range(K):
        new_representatives.append(representatives[i] + P * (centroids[i] - representatives[i]))
    new_representatives = np.array(new_representatives).reshape([K, N, 2])

    # for i in range(K):
    #     plt.plot(new_representatives[i][:, 0], new_representatives[i][:, 1],
    #              color=(np.random.uniform(0, 1), np.random.uniform(0, 1), np.random.uniform(0, 1)), marker='o',
    #              markersize=7, linestyle=' ')
    # plt.plot(data[:, 0], data[:, 1],
    #          color=(np.random.uniform(0, 1), np.random.uniform(0, 1), np.random.uniform(0, 1)), marker='o',
    #          markersize=2, linestyle=' ')

    #savefig('Representatives_After_Movements')
    #plt.close()
    # print '########Representatives After Movements#############'
    # print new_representatives
    return new_representatives


def cluster_full_data(full_data_path, new_representatives):
    full_data = read_data(full_data_path)
    new_representatives = new_representatives.reshape([-1, 2])
    neigh = NearestNeighbors(n_neighbors=1)
    neigh.fit(new_representatives)
    ind = neigh.kneighbors(full_data, return_distance=False)
    ind = np.floor(ind / N).astype('int')
    ind = ind.reshape([full_data.shape[0]])

    for i in range(full_data.shape[0]):
        with open(output_file, 'a') as the_file:
            the_file.write('{},{},{}\n'.format(full_data[i, 0], full_data[i, 1], ind[i]))

    return


if __name__ == "__main__":
    data = read_data(sample_data_path)
    initial_labels, centroids = hierarchical_clustering(data, K)
    # print '##########Initial Centroids##########'
    # print centroids
    # print '######################################'
    #save_fig(data, initial_labels, K)
    representatives = get_representatives(data, initial_labels, N)
    new_representatives = get_new_representatives(representatives, P)
    cluster_full_data(full_data_path, new_representatives)
    print representatives

