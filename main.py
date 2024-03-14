from flask import Flask, jsonify, render_template, request, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import create_engine
import networkx as nx
import io
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:1234@localhost:5432/Agent Hunt'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class Brokerage(db.Model):
    __tablename__ = 'brokerage'
    id = db.Column(db.Integer, primary_key=True)
    national_association_id = db.Column(db.String(32))
    email = db.Column(db.String(128))
    name = db.Column(db.String(128))
    short_name = db.Column(db.String(128))
    street = db.Column(db.String(512))
    city = db.Column(db.String(128))
    county = db.Column(db.String(128))
    state = db.Column(db.String(128))
    zipcode = db.Column(db.String(10))
    phone_numbers = db.Column(db.String(128), default='[]')
    url = db.Column(db.String(256))
    status = db.Column(db.String(128))

class AgentInfo(db.Model):
    __tablename__ = 'agent_info'
    id = db.Column(db.Integer, primary_key=True)
    national_association_id = db.Column(db.String(32))
    state_license = db.Column(db.String(32))
    email = db.Column(db.String(128))
    first_name = db.Column(db.String(128))
    street = db.Column(db.String(512))
    city = db.Column(db.String(128))
    county = db.Column(db.String(128))
    state = db.Column(db.String(128))
    zipcode = db.Column(db.String(10))
    phone_numbers = db.Column(db.String(128), default='[]')
    status = db.Column(db.String(128))
    brokerage_id = db.Column(db.Integer, db.ForeignKey('brokerage.id'))

class HomeInfo(db.Model):
    __tablename__ = 'home_info'
    id = db.Column(db.Integer, primary_key=True)
    state_market_id = db.Column(db.Integer)
    county_market_id = db.Column(db.Integer)
    city_market_id = db.Column(db.Integer)
    zipcode_market_id = db.Column(db.Integer)
    neighborhood_level_1_market_id = db.Column(db.Integer)
    neighborhood_level_2_market_id = db.Column(db.Integer)
    on_market_knwl_time = db.Column(db.DateTime)
    pending_knwl_time = db.Column(db.DateTime)
    last_sold_knwl_time = db.Column(db.DateTime)

class AgentListing(db.Model):
    __tablename__ = 'agent_listing'
    id = db.Column(db.Integer, primary_key=True)
    deal_side = db.Column(db.String(32), nullable=False)
    is_primary = db.Column(db.Boolean, nullable=False)
    home_id = db.Column(db.Integer, db.ForeignKey('home_info.id'))
    status = db.Column(db.String(255))
    listing_price = db.Column(db.Numeric(12,2))
    last_sold_price = db.Column(db.Numeric(12,2))
    agent_id = db.Column(db.Integer, db.ForeignKey('agent_info.id'))

@app.route('/erd')
def generate_erd():
    engine = create_engine('postgresql://postgres:1234@localhost:5432/Agent Hunt')
    graph = create_schema_graph(metadata=db.metadata, engine=engine)
    graph.write_png('erd.png')
    return send_file('erd.png', mimetype='image/png')

def identify_unique_records():
    unique_agents = {}
    unique_brokerages = {}
    unique_brokerage_branches = {}

    # Iterate through AgentInfo records
    agent_records = AgentInfo.query.all()
    for agent in agent_records:
        if agent.id not in unique_agents:
            unique_agents[agent.id] = {
                'id': agent.id,
                'national_association_id': agent.national_association_id,
                'state_license': agent.state_license,
                'email': agent.email,
                'first_name': agent.first_name,
                'street': agent.street,
                'city': agent.city,
                'county': agent.county,
                'state': agent.state,
                'zipcode': agent.zipcode,
                'phone_numbers': agent.phone_numbers,
                'status': agent.status,
                'brokerage_id': agent.brokerage_id
            }

    # Iterate through Brokerage records
    brokerage_records = Brokerage.query.all()
    for brokerage in brokerage_records:
        if brokerage.id not in unique_brokerages:
            unique_brokerages[brokerage.id] = {
                'id': brokerage.id,
                'national_association_id': brokerage.national_association_id,
                'email': brokerage.email,
                'name': brokerage.name,
                'short_name': brokerage.short_name,
                'street': brokerage.street,
                'city': brokerage.city,
                'county': brokerage.county,
                'state': brokerage.state,
                'zipcode': brokerage.zipcode,
                'phone_numbers': brokerage.phone_numbers,
                'url': brokerage.url,
                'status': brokerage.status
            }

    # Iterate through HomeInfo records (assuming HomeInfo represents brokerage branches)
    home_records = HomeInfo.query.all()
    for home in home_records:
        if home.id not in unique_brokerage_branches:
            unique_brokerage_branches[home.id] = {
                'id': home.id,
                'state_market_id': home.state_market_id,
                'county_market_id': home.county_market_id,
                'city_market_id': home.city_market_id,
                'zipcode_market_id': home.zipcode_market_id,
                'neighborhood_level_1_market_id': home.neighborhood_level_1_market_id,
                'neighborhood_level_2_market_id': home.neighborhood_level_2_market_id,
                'on_market_knwl_time': home.on_market_knwl_time,
                'pending_knwl_time': home.pending_knwl_time,
                'last_sold_knwl_time': home.last_sold_knwl_time
            }

    return {
        'unique_agents': list(unique_agents.values())[:10],
        'unique_brokerages': list(unique_brokerages.values())[:10],
        'unique_brokerage_branches': list(unique_brokerage_branches.values())[:10]
    }

@app.route('/display_records')
def display_records():
    unique_records = identify_unique_records()
    return jsonify(unique_records)

def build_relationship_graph():
    G = nx.Graph()
    agent_listings = AgentListing.query.limit(2000).all()

    for listing in agent_listings:
        agent_id = listing.agent_id
        if not G.has_node(agent_id):
            G.add_node(agent_id)
        
        other_agents = AgentListing.query.filter(AgentListing.home_id == listing.home_id).filter(AgentListing.id != listing.id).all()
        
        for other_agent in other_agents:
            other_agent_id = other_agent.agent_id
            if not G.has_edge(agent_id, other_agent_id):
                G.add_edge(agent_id, other_agent_id)

    return G

@app.route('/relationship_graph')
def relationship_graph():
    G = build_relationship_graph()
    
    pos = nx.spring_layout(G) 
    plt.figure(figsize=(100, 100))
    nx.draw(G, pos, with_labels=True, node_size=500, node_color='skyblue', font_size=10)
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    
    return send_file(img, mimetype='image/png')

@app.route('/market_info/<int:market_id>/<int:top_n>')
def market_info(market_id, top_n):
    top_agents = db.session.query(AgentInfo).join(AgentListing).join(HomeInfo).filter(HomeInfo.city_market_id == market_id).order_by(AgentInfo.id).limit(top_n).all()
    top_brokerages = db.session.query(Brokerage).join(AgentInfo).join(AgentListing).join(HomeInfo).filter(HomeInfo.city_market_id == market_id).order_by(Brokerage.id).limit(top_n).all()

    return render_template('market_info.html', top_agents=top_agents, top_brokerages=top_brokerages)

def build_relationship_graph1(market_id):
    G = nx.Graph()
    agent_listings = AgentListing.query.join(HomeInfo).filter(HomeInfo.city_market_id == market_id).limit(2000).all()

    for listing in agent_listings:
        agent_id = listing.agent_id
        if not G.has_node(agent_id):
            G.add_node(agent_id)
        
        other_agents = AgentListing.query.filter(AgentListing.home_id == listing.home_id).filter(AgentListing.id != listing.id).all()
        
        for other_agent in other_agents:
            other_agent_id = other_agent.agent_id
            if not G.has_edge(agent_id, other_agent_id):
                G.add_edge(agent_id, other_agent_id)

    return G

@app.route('/relationship_graph/<int:market_id>')
def visualize_relationship_graph(market_id):
    G = build_relationship_graph1(market_id)
    
    pos = nx.spring_layout(G) 
    plt.figure(figsize=(100, 50))
    nx.draw(G, pos, with_labels=True, node_size=500, node_color='skyblue', font_size=10)
    
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    
    return send_file(img, mimetype='image/png')

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
