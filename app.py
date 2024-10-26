from flask import Flask, render_template, request, redirect, url_for
from pyspark_jobs.flight_delay_analysis import analyze_flight_delay
from pyspark_jobs.carrier_performance import carrier_performance_route
from pyspark_jobs.flight_route_analysis import analyze_flight_routes
from pyspark_jobs.customer_satisfaction import analyze_customer_satisfaction
from pyspark_jobs.cancellations_weather import analyze_cancellations_weather

app = Flask(__name__)

# Welcome page route
@app.route('/')
def welcome():
    return render_template('welcome.html')

# Main project page with five buttons
@app.route('/main_page')
def main_page():
    return render_template('main.html')

# Route for Flight Delay Analysis
@app.route('/flight_delay', methods=['GET', 'POST'])
def flight_delay():
    if request.method == 'POST':
        FlightNum = request.form['FlightNum']
        result = analyze_flight_delay(FlightNum)
        return render_template('flight_delay.html', result=result)
    return render_template('flight_delay.html', result=None)

# Route for Carrier Performance Comparison
@app.route('/carrier_performance', methods=['GET', 'POST'])
def carrier_performance_view():
    if request.method == 'POST':
        # Call the PySpark function for carrier performance analysis
        result = carrier_performance_route()  
        return render_template('carrier_performance.html', result=result)
    return render_template('carrier_performance.html', result=None)

# Route for Popular Flight Routes Analysis
@app.route('/popular_routes', methods=['GET', 'POST'])
def popular_routes():
    if request.method == 'POST':
        result = analyze_flight_routes()
        return render_template('flight_route.html', result=result)
    return render_template('flight_route.html', result=None)

# Route for Customer Satisfaction Insights
@app.route('/customer_satisfaction', methods=['GET', 'POST'])
def customer_satisfaction():
    if request.method == 'POST':
        result = analyze_customer_satisfaction()
        return render_template('customer_satisfaction.html', result=result)
    return render_template('customer_satisfaction.html', result=None)

# Route for Cancellations and Weather Impact
@app.route('/cancellations_weather', methods=['GET', 'POST'])
def cancellations_weather():
    if request.method == 'POST':
        # Access the input correctly from the form data
        FlightNum = request.form.get('FlightNum')  # Using .get() to avoid KeyError
        if FlightNum:  # Check if FlightNum is provided
            result = analyze_cancellations_weather(FlightNum)  # Pass FlightNum to your analysis function
            return render_template('cancellations_weather.html', result=result)
        else:
            return render_template('cancellations_weather.html', result="Please provide a Flight Number.")
    return render_template('cancellations_weather.html', result=None)

# Error handling for page not found
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.route('/routes')
def list_routes():
    output = []
    for rule in app.url_map.iter_rules():
        output.append(f"{rule.endpoint}: {rule.rule}")
    return '<br>'.join(output)

if __name__ == '__main__':
    app.run(debug=True)
