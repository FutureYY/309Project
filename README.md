# EGT309_Project

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

Made by yours truly, Women In Stem

Members:

Chua Jiawen 231836H - 231836H@mymail.nyp.edu.sg 

Goh Xin Leng Christel 220980C - 220980C@mymail.nyp.edu.sg 

Wong Yen Yi 230666M - 230666M@mymail.nyp.edu.sg 

Yoshana Magendran 230591E - 230591E@mymail.nyp.edu.sg

## Programming language, librabries and packages used



## EDA Key findings

### Data Quality Issues
#### Zero installments in payments
Before data cleaning was done, I had explored the datasets and found some interesting points about the given datasets. Starting off with “olist_order_payments”, I discovered that there were two transactions which had 0 as their number of installments. Logically speaking, if the purchase was paid in full, the number of installments would be 1 instead of 0. As this error was a very small fraction of the dataset (2 out of 103,875 transactions = 0.00193%), I decided to remove these 2 transactions, as it may have just been a data entry error, to maintain data integrity. 

#### Invalid review timestamps
Another error that I discovered was incorrect timestamp entries, where the review was made before order was delivered. When checking if the dates for delivery was after the order_approved_at date, I joined it with olist_order_reviews to check if the “review_creation_date” was after “order_delivered_customer_date”. There were several entries where reviews were created before their order was delivered. Firstly, this affects the accuracy of the review score given as the customer did not review it based on the delivery speed or product. Secondly, a customer would not be able to review a product before receiving it. Hence, these rows were removed as it may affect sentiment or customer satisfaction analysis.

### Outlier Detection & Feature Engineering
#### High installment transactions
While checking for outliers, I discovered payment transactions which were extremely high in value. To help the model better understand and handle these values, I engineered a function, “high_installment_flag”, to categorize installments into 1 or 0, where 1 indicates that the installment value is more unusual compared to the usual transactions. 

A high installment flag is assigned to orders when: 
- The number of installments is above average, or
- The value of per installment paid is lower than average, even after outlier capping. 

The flags were assigned using the IQR method to cap outliers, followed by comparing each transaction’s capped value to the overall average value. This feature will help the model to understand customer behaviour, which can identify customers stretching out payments or opting for long-term installment plans.


#### Voucher Usage
Another feature created was ‘used_voucher’, which identifies whether a customer has used a discount voucher for their payment. This gives insights into customer purchasing power and customer loyalty as:
- Customers who are using vouchers for their payments are not paying for the item directly or are purchasing at a discounted rate, suggesting lower purchasing power or a higher sensitivity to price. 
- These customers are drawn in by promotions and are less likely to make repeat purchases without discounts, affecting customer loyalty and retention.
Hence, vouchers may attract more customers, but may not retain them permanently.


### EDA Visualisations & Insights
#### Review Score Distribution by Delivery Speed
![Alt text](images\Review_Score_Distribution_by_Delivery_Speed.png)
This boxplot shows the distribution of review score (1-5) across the delivery speed categories (fast, normal and slow). Based on the graph, the IQR for fast delivery speed is between 4.0 to 5.0, with a few outliers of 1 and 2 stars reviews. For normal delivery speed, the IQR is ranging from 3.0 to 5.0, however the whiskers reaches down to 1.0. For slow delivery speed, although the median is around 4.0, the IQR stretches from 1.0 to 5.0. From this, it shows that fast deliveries generally lead to a higher satisfaction rate, while slow deliveries cause customers to be unpredictable, leading to a wide range of review scores and also low reviews. Hence, delivery speed will affect customer experience, no matter what product it is.

#### Distribution of Days between Repeat Orders (binned)
![Alt text](images\Distribution_of_Days_Between_Repeat_Orders.png)
The bar graph shows how frequent customers place orders, from their last order till their latest one. From this graph, the range of days which have the highest number of repeat customers are 90-180 days, followed by 180-365 days. Bins from 14-30 days and 60-90 days also have a high volume of repeat orders, suggesting that a large number of customers reorder within 1 to 3 months. 

On the contrary, there are little orders made within 1 week, indicating that the platform does not sell products/services that are consumable on a daily/weekly basis. With a higher concentration of buyers reordering during the 3-12 months period, it shows that long-tem engagement would be much more effective in retaining customers than focusing on swift purchases, due to the nature of products being sold on this platform.

#### Repurchase Behaviour of Voucher Users
![Alt text](images\Repurchase Behavior of Voucher Users.png)
The bar graph shows the number of voucher users who are repeat and non-repeat buyers. The majority of voucher users are non-repeat, while only a small percentage of voucher users are repeat buyers. This shows that vouchers are effective in bring customers to the platform to purchase items, but are ineffective in retaining them, as many are non-repeat buyers.

#### Correlation matrix
![Alt text](images\correlation matrix.png)
With the red box having a strong positive correlation, most of the features are white (no correlation). Initially, I did one-hot encoding on product categories aas our model could only take in numerical values. However, I decided to remove most of the features which were the one-hot encoded categories as they were mostly white. 

Features like “delivered_in_days” had a strong correlation with: 
a. “Delivery_distance_in_km” (longer distance = longer delivery time)
b. “delivery_speed” (slower speed = more days taken to deliver)
c. “month of purchase” (different seasons may affect delivery time)

“High_installment_flag” is also strongly correlated with “installment_value” which could be because it was derived from installment_value itself. 


## Instructions for executing the pipeline

1. Run Ubuntu or any Linux terminal on your device
2. Path the file using cd

```
cd /mnt/file_path

```
e.g. cd /mnt/c/Users/YourName/Documents

3. Create a virtual environment
   
```
python -m venv venv
```

4. Activate virtual environment
 
```
venv/bin/activate
```

5. Install the dependencies by running

```
pip install --upgrade pip
pip install -r requirements.txt
```

6. Run the project using 

```
bash run.sh
```

## Flow of pipeline

## Choice of machine learning model

## Model evaluation

## Other Consideration





## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a data engineering convention
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to test your Kedro project

Have a look at the file `src/tests/test_run.py` for instructions on how to write your tests. You can run your tests as follows:

```
pytest
```

You can configure the coverage threshold in your project's `pyproject.toml` file under the `[tool.coverage.report]` section.


## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. You can install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `context`, 'session', `catalog`, and `pipelines`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
