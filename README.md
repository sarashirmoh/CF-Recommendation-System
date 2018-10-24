# CF-Recommendation-System
Item-based collaborative filtering system using Spark MLlib., to predict movie rating for MovieLens dataset.
Used Pearson Correlation to measure movies' similarities.
Cold start, ie. Unique movies, and Out of range problems are handled by replacing average with median rate.
To optimize the runtime and avoid JAVA heap space error, only correlations above zero are saved, which also lead to better accuracy.
RMSE = 1.0102704251085006
