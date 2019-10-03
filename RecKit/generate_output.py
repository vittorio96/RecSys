import numpy as np
import pandas as pd
def generate_output(targetList, recommended_items, path="submission.csv"):
    stringV = [np.array2string(x)[1:-1] for x in recommended_items]
    df = pd.DataFrame(np.column_stack((targetList, stringV)), columns=['playlist_id', 'track_ids'])
    df.to_csv(path, index=False)