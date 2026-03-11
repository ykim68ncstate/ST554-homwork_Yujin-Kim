
#import some modules needed
import matplotlib.pyplot as plt
import numpy as np
from numpy.random import default_rng
from sklearn import linear_model

class SLR_slope_simulator:
    def __init__(self, beta_0, beta_1, x, sigma, seed):
        self.beta_0 = beta_0
        self.beta_1 = beta_1
        self.sigma = sigma
        self.x = x
        self.n = len(x) # number of data
        self.rng = default_rng(seed) # save random seed generation
        self.slopes = [] # create empty list for saving result
        
    def generate_data(self):
        y = self.beta_0 + self.beta_1 * self.x + self.rng.normal(0, self.sigma, self.n) #self.rng.normal: generate random noise
        return self.x, y

    def fit_slope(self, x, y):
        reg = linear_model.LinearRegression() #create LinearRegression() object
        fit = reg.fit(x.reshape(-1,1), y) # x.reshpae: fit() of sklearn only receives 2 dimensions as an input
        return fit.coef_[0] # return slope coefficient

    def run_simulations(self, num_simulations):
        slopes_list = [] #create an empty list for saving results from each simulation

        for i in range(num_simulations): # repeat as much as num_simulation value
            x, y = self.generate_data() # call generate_data method, create a dataset
            slope = self.fit_slope(x, y) # run fit_slope() method, fit simple linear regression, and then return the slope
            slopes_list.append(slope) # append(): add a value at once in the list ;add slope value in end of the list

        self.slopes = np.array(slopes_list) #after all repetition, change the slope value to a numpy array

    def plot_sampling_distribution(self):
        if len(self.slopes) == 0: # At the first time, there are no slopes due to self.slopes = []
           print("run_simulations() must be called first")
        else:
           plt.hist(self.slopes)
           plt.xlabel("Estimated slope")
           plt.ylabel("Frequency")
           plt.title("Sampling Distribution of the Slope Estimator")
           plt.show()

    def find_prob(self, value, sided):
        if len(self.slopes) ==0:
            print("run_simulations() must be called first")
            return None

        if sided == "above": # If sided is “above”
          return np.mean(self.slopes > value) # the probability of being larger than the value.

        elif sided == "below": #If it is “below”
          return np.mean(self.slopes < value) # the probability of being smaller than the value.

        elif sided == "two-sided": #If it is “two-sided”
          median_value = np.median(self.slopes) # check if the value is above or below the median. so set the median_value

          if value > median_value:   #If above
              return 2 * np.mean(self.slopes > value) #two times the probability of being larger
          else: #if below
              return 2 * np.mean(self.slopes < value) #two times the probability of being smaller.
