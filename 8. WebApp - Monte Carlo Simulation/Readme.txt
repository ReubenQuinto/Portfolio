Objective: 
	1. Calculate the probability of a 1 day yield of 5 different stocks.

The solution: 
	- I created created a Monte Carlo Simulation to create simulated 1 day possible yields based on based on historical close prices.
	- Yields are created by using a python server which calculates: the mean and standard deviation of daily percentage changes on closing prices. Those parameters are fed into a Gaussian Probability Function which outputs the yield.
	- The Monte Carlo Simulator is a web-application.
	- The user can interact with the app by inputting: which stock they want to observe, how many times they want to simulate an outcome, and the confidence interval of an area of interest.
	- The web application is deployed on Google Cloud Platform.

Key Takeaways: 
	- This project is my first full-stack statistical web application.
	- The math was relatively easy. I just needed to calculate daily percent changes, mean, and standard deviation. Pass that information to a Gaussian function for-loop.
	- This project has increased my proficiency with: Python, Pandas, making custom python libraries, JavaScript, Get/Post, JSON, Plotly, HTML, Jinja, Flask, GCP, developing statistical applications, and allowing a user to interact with the application.