import streamlit as st
import psycopg2
import networkx as nx
from pyvis.network import Network
import tempfile
import io

# Database connection parameters
host = host
port = '5432'
dbname = demodb
user = 'postgres'
password = 'ps'

def fetch_data(query):
    """Fetch data from PostgreSQL database."""
    conn = None
    data = []
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return data

def create_graph(hub_data, satellite_data):
    """Create an interactive graph from hub and satellite data."""
    G = nx.Graph()

    # Add nodes and edges for hub table
    for employee_id in hub_data:
        G.add_node(employee_id[0], type='hub')

    # Add nodes and edges for satellite table
    for row in satellite_data:
        employee_id = row[0]
        G.add_node(employee_id, type='satellite', **dict(zip([
            'first_name', 'last_name', 'email', 'phone_number',
            'hire_date', 'job_id', 'salary', 'commission_pct',
            'manager_id', 'department_id'
        ], row[1:])))
        G.add_edge(employee_id, employee_id)  # Self-loop to visualize

    return G

def visualize_graph(G):
    """Visualize the graph using Pyvis and return HTML path."""
    net = Network(height='750px', width='100%', bgcolor='#222222', font_color='white')

    for node in G.nodes(data=True):
        net.add_node(node[0], label=str(node[0]), title=str(node[1]), color='blue' if node[1]['type'] == 'hub' else 'red')

    for edge in G.edges():
        net.add_edge(edge[0], edge[1])

    # Create a temporary file to save the HTML
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.html')
    file_path = temp_file.name
    net.save_graph(file_path)

    return file_path

def main():
    st.title("Interactive Graph from Hub and Satellite Tables")

    # Fetch data from the database
    hub_data = fetch_data("SELECT employee_id FROM employee_hub;")
    satellite_data = fetch_data("""
        SELECT employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id
        FROM employee_satellite;
    """)

    if hub_data and satellite_data:
        # Create the graph
        G = create_graph(hub_data, satellite_data)
        
        # Visualize the graph
        graph_file_path = visualize_graph(G)
        
        # Render the graph in Streamlit
        st.write("### Graph Visualization")
        with open(graph_file_path, 'r') as file:
            graph_html = file.read()
        st.components.v1.html(graph_html, height=800, width=800)

        # Clean up the temporary file
        os.remove(graph_file_path)

if __name__ == "__main__":
    main()
