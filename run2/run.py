import matplotlib.pyplot as plt
import matplotlib

# Set the font to one that supports Chinese, e.g. SimHei (bold) Project Objective
matplotlib.rcParams['font.family'] = 'SimHei'  # Bold
matplotlib.rcParams['axes.unicode_minus'] = False  # Solve the problem of the negative sign ‘-’ being displayed as a square


import run1
import run2
import run3

x_values1 = [100, 1000, 5000]
y1_values = []
y2_values = []
y3_values = []
for index1, model in enumerate([run1, run2, run3]):
    print("model: ", model)
    y1_values.append([])
    y2_values.append([])
    y3_values.append([])
    for index2, param in enumerate(x_values1):
        collect_time1 = model.RUN(param, param).collect_time()
        # print(collect_time1)
        y1_values[-1].append(collect_time1[0])
        y2_values[-1].append(collect_time1[1])
        y3_values[-1].append(collect_time1[2])
    # collect_time2 = model.RUN(300, 300).collect_time()
    # collect_time3 = model.RUN(500, 500).collect_time()

print("y1_values: ", y1_values)
print("y1_values: ", y2_values)
print("y1_values: ", y3_values)

def draw(y_values1, y_values2, y_values3):
    # Create the first line chart
    plt.figure(figsize=(10, 6))
    plt.plot(x_values1, y_values1, marker='o', label='line1', color='b')  # First line
    plt.plot(x_values1, y_values2, marker='s', label='line2', color='g')  # Second line
    plt.plot(x_values1, y_values3, marker='^', label='line3', color='r')  # Third line

    # Add titles and tags
    plt.title('Different semantic runtimes')
    plt.xlabel('quantities')
    plt.ylabel('times')

    # Add legend
    plt.legend()

    # Display grid
    plt.grid()

    # Display the first figure
    plt.show()

for index, value in enumerate(x_values1):
    draw(y1_values[index], y2_values[index], y3_values[index])