import raft

processors = 3  # Number of processors
task_duration = [2, 1, 3] # In Seconds

db_config = {
        "host":"localhost",
        "user":"root",
        "password":"arb12345",
        "database":"processors"
}

nodes = [raft.Node(i, processors, db_config) for i in range(processors)]

for i, node in enumerate(nodes):
  node.task = f"Task {i+1}"
  node.task_time = task_duration[i]
  node.nodes = nodes

raft.Node.run(nodes)
