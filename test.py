import pandas as pd
import networkx as nx
from pyvis.network import Network


exclude_servers = {'Ansible-SRV', 'SCCM'}  

group_colors = {
    "Prod": "red",
    "Test": "blue",
    "Archive": "brown",
    "IT": "gray",
    "General": "green"
}

df = pd.read_excel("many_ip.xlsx")
df = df[['Server_name', 'Serv_group', 'OS_Serv', 'Dest', 'Dest_group', 'OS_Dest', 'IP_source', 'IP_dest']]

def get_text_color(os_value):
    if os_value == "Linux":
        return "black"
    elif os_value == "Windows":
        return "blue"
    return "gray"

edges = set()
nodes = {}
node_mapping = {}

filtered_df = df[~df['Server_name'].isin(exclude_servers)]
filtered_dest_df = df[~df['Dest'].isin(exclude_servers)]

for _, row in filtered_df.iterrows():
    node1_base = f"{row['Server_name']} ({row['Serv_group']})"
    node2_base = f"{row['Dest']} ({row['Dest_group']})"
    

    if row['Dest'] not in exclude_servers:
        node1_full = f"{node1_base}\n{row['IP_source']}"
        node2_full = f"{node2_base}\n{row['IP_dest']}"
        
        node_mapping[node1_base] = node1_full
        node_mapping[node2_base] = node2_full
        
        nodes[node1_full] = {
            "color": group_colors.get(row['Serv_group'], "white"),
            "font_color": get_text_color(row['OS_Serv'])
        }
        
        nodes[node2_full] = {
            "color": group_colors.get(row['Dest_group'], "white"),
            "font_color": get_text_color(row['OS_Dest'])
        }
        
        if node1_full != node2_full:
            edges.add((node1_full, node2_full))

G = nx.DiGraph()
net = Network(notebook=True, directed=True, height="900px", width="100%", bgcolor="#FFFFFF")

for node, props in nodes.items():
    net.add_node(node, label=node, color=props["color"], font={"color": props["font_color"], "size": 12}, size=10)

for edge in edges:
    net.add_edge(edge[0], edge[1])

net.set_options("""
const options = {
    "edges": {
        "arrows": {
            "to": {
                "enabled": true,
                "scaleFactor": 0.45
            }
        },
        "color": {
            "inherit": true
        },
        "selfReferenceSize": null,
        "selfReference": {
            "angle": 0.7853981633974483
        },
        "smooth": {
            "forceDirection": "none"
        }
    },
    "physics": {
        "barnesHut": {
            "avoidOverlap": 0.1
        },
        "minVelocity": 0.75
    }
}
""")

net.show("graph.html")