Objective: 
	1. Calculate the probability of a 1 day yield of 5 different stocks.

The Solution: 
	- 1 day yields are generated on the python/flask server by: calculating percent differences on adj closing prices. Then extracting, from the percent change array, the average and standard deviation. Those parameters are passed into a Gaussian/normal probability density function and the output is a simulated yield. I've wrote a custom python library to get the array of simulated output to create buckets and frequency counts, then post that information to an api I created in Flask. The front-end interacts with the api through AJAX POST requests.
	- The end-user on the front-end can input: which stock to select, how many times to run the simulation, an area of interest, and a confidence level.
	- The Monte Carlo Simulator is a statistical web application.
	- The web application is deployed on Google Cloud Platform.

Key Takeaways: 
	- This project is my first full-stack statistical web application.
	- The math side I calculated daily percent changes, mean, standard deviation, then pass that information to a Gaussian function within a for-loop.
	- On the coding side this had increased my proficiency with: Python, Pandas, making custom python libraries, JSON, JavaScript, GET/POST, AJAX, Plotly, HTML, Jinja, Flask, GCP, developing statistical applications, and allowing a user to interact with the application.