def drop_nan_value_rows(df:pd.DataFrame) -> pd.DataFrame:
    '''
    Dropping Rows if there are fully empty.
    ''' 
    df = df.dropna(how='all')
    return df

def drop_global_duplicated(df:pd.DataFrame, col:str)-> pd.DataFrame:
    '''
    searching for dupliccateds in any columns and keep only the first occurrence of each duplicated row 
    '''
    df = df.drop_duplicates(subset=col)
    return df

def replace(df:pd.DataFrame, col:str, replace:str, replaced_by:str)-> pd.DataFrame:
    '''
    replace a string for another string
    ''' 
    if data[col].dtype == 'O':
        df[col] = df[col].str.replace(replace,replaced_by)
        return df

def fillna(df:pd.DataFrame, col:str, fill:str)->pd.DataFrame:
    '''
    Fill all the NaNs of one column with the median of that column
    '''
    if fill == 'median':
        clv_mean = df[col].median()
        df[col] = df[col].fillna(clv_mean)
        return df
    if fill == 'mode':
        clv_mode = df[col].mode()
        df[col] = df[col].fillna(clv_mode)
        return df
    else:
        df[col] = df[col].fillna(fill)
        return df

def chance_datatype(df:pd.DataFrame, col=str, d_type=type)->pd.DataFrame:
    '''
    chancing the datatype of a column 
    '''
    df[col] = df[col].astype(d_type)
    return df

def split(df: pd.DataFrame, symbol:str, col:str)-> pd.DataFrame:
    '''
    split a string by specific symbol and indexing which split should return
    ''' 

    df2 = df.copy()
    if data[col].dtype == 'O':
        if col not in df2.columns:
            return df2
        else:
            df2[col] = df2[col].apply(lambda x: x.split(symbol)[1] )
            
            return df2

def clean_gender_column(df: pd.DataFrame) -> pd.DataFrame:
    '''
    This function will take a Pandas DataFrame as an input and it will replace the values in
    the "gender" column ins such a way that any gender which is not Male or Female with be 
    replaced by "U" otherwise the genders will be either "F" or "M"

    Inputs:
    df: Pandas DataFrame

    Outputs:
    A pandas DataFrame with the values in the "gender" column cleaned.
    '''

    df2 = df.copy()

    if "gender" not in df2.columns:
        return df2
    else:
        #df2['gender'] = df2['gender'].apply(lambda x: x[0].upper() if x[0].upper() in ['M', 'F'] else "U")
        df2['gender'] = list(map(lambda x: x[0].upper() if x[0].upper() in ['M', 'F'] else "U", df2['gender']))
        return df2
    
def data_clean(df: pd.DataFrame) -> pd.DataFrame:
    '''
    start all cleaning functions
    '''
    df = drop_nan_value_rows(df)   
    df = drop_global_duplicated(df,'customer')
    df = replace(df,'education','Bachelors','Bachelor')
    df = replace(df,'state','AZ','Arizona')
    df = replace(df,'state','Cali','California')
    df = replace(df,'state','WA','Washington')
    df = replace(df,'vehicle_class','Sports Car','Luxury')
    df = replace(df,'vehicle_class','Luxury SUV','Luxury')
    df = replace(df,'vehicle_class','Luxury Car','Luxury')
    df = replace(df,'customer_lifetime_value','%','')
    df = fillna(df,'customer_lifetime_value','median')
    df = chance_datatype(df,'customer_lifetime_value',float)
    df = fillna(df,'gender','U')
    df = clean_gender_column(df)
    df = split(df,'/','number_of_open_complaints')
    df = chance_datatype(df,'number_of_open_complaints',int)
    return df
