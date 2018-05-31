import json
import ast
import matplotlib.pyplot as plt

speeds = {}
with open('average_velocity.txt') as file:
    for line in file:
        line = ast.literal_eval(line)
        if not line['weekday'] in speeds:
            speeds[line['weekday']] = []
        speeds[line['weekday']].append((line['hour'], line['speed']))

plt.figure(figsize=(16, 8))
for day in speeds:
    hours, velocities = zip(*speeds[day])
    plt.plot(hours, velocities, label=day)
plt.legend()
plt.xlabel('Hour')
plt.ylabel('Velocity, km/h')
plt.title('Average car velocity in Moscow')
plt.savefig('Average_velocities.png')
plt.show()
