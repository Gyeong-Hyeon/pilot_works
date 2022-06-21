This package will help you to create Pandas dataframe with your Redshift data with just 2 lines of Python codes.


## How to use
1. Install this package with pip CLI.
    ```bash
    $ pip install red2df
    ```

2. Make Redshift db cursor with your Redshift information.
    ```python
    from red2df import RedshiftToDf

    DB_NAME = "your DB name"
    USER = "your user name"
    PASSWORD = "your password"
    HOST = "your Redshift endpoint url"
    #PORT = your port number in integer. Default is 5439

    #create an instance
    cur = RedshiftToDF(DB_NAME, USER, PASSWORD, HOST)
    ```

3. Run SQL queries and print dataframe.
    ```python 
    df = cur.create_df("your query")
    print(df)

    """
    example of result:
    +----+-----------------------+
    |    | user_data__platform   |
    |----+-----------------------|
    |  0 | ANDROID_APP           |
    |  1 | ANDROID_APP           |
    +----+-----------------------+
    """
    ```

4. You also can save the result of query in csv. Please give `save_csv=True` option when you run `create_df()`.
    * The parameters of `create_df()`:
        * `sql`[str] : SQL query you want to run
        * `save_csv`[bool] : Save the result of the given query in csv if True. Default is False
        * `file_path`[str] : Path of csv file. Default is './df.csv'.

Please feel free to email me if there is any problem - hyunie@tumblbug.com
