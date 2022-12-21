import pandas as pd
import time


def timeit(function):
    """
    

    Parameters
    ----------
    function : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """

    def timed(*args, **kwargs):
        ts = time.time()
        result = function(*args, **kwargs)
        te = round((time.time() - ts), 3)

        print('__TIMER__ Function {} ran in: {}'.format(function.__name__, te))
        return result

    return timed


def clean_index(df_input):
    df_input.reset_index(drop=True, inplace=True)
    df_input.drop("Unnamed: 0", axis=1, inplace=True)

    return df_input


def change_col_type_to_string(df, col_name):
    """
    

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    col_name : TYPE
        DESCRIPTION.

    Returns
    -------
    df : pandas Dataframe
        Return dataframe with changed type for selected column.
        Removes trailing zeros for integers.
        New column type is string.
        

    """
    # Remove trailing zeros
    df[col_name] = df[col_name].fillna('').astype(str).str.replace(".0", "", regex=False)

    return df


def filter_on_value_in_column(df, col_name, row_value):
    """
    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    col_name : TYPE
        DESCRIPTION.
    row_value : TYPE
        DESCRIPTION.

    Returns
    -------
    output_df : pandas Dataframe
        Return dataframe with filtered values.

    """

    output_df = df[df[col_name] == row_value]

    return output_df


@timeit
def iterate_on_two_dataframes(df_input, df_target):
    """
    

    Parameters
    ----------
    df_input : TYPE
        DESCRIPTION.
    df_target : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    # List to store updated parts
    # For review only
    updated_parts_list = []

    # New column for reference
    df_target["RULE_APPLIED"] = ""
    # Temp dataframe with filtered empty N_POS
    df_target_only_null = df_target[df_target["N_POS"].isnull()]

    for i, row in df_input.iterrows():
        key_to_match = row['N_PRT_TRC_ID']

        # Get value from cell using index and col name
        value_to_paste = df_input.at[i, 'N_POS']

        # Get index when the key matches
        # Search for index in filtered dataframe with less rows
        # Indexes are the same in filtered and full dataframe
        # This approach shorten run time ~20x
        idx = df_target_only_null[df_target_only_null["N_PRT_TRC_ID_IN"] == key_to_match].index

        # Index is not 0 if the key is found in target dataframe
        if idx.size != 0:
            print(f"Key to match {key_to_match}")

            # Setting value
            df_target.loc[df_target.index[idx], 'N_POS'] = value_to_paste
            # Setting value for traceability
            df_target.loc[df_target.index[idx], 'RULE_APPLIED'] = "X"

            updated_parts_list.append(key_to_match)

    return df_target, updated_parts_list


if __name__ == '__main__':
    # Read data
    df_ta009 = pd.read_csv("TA009_TA002_TA001_GIB_ORA QA_N_POS_assigned_light.csv")
    df_ta061 = pd.read_csv("TA061_TA002_ORA QA.csv")

    # Change column type to avoid errors with types
    df_ta009 = change_col_type_to_string(df=df_ta009,
                                         col_name="N_PRT_TRC_ID")

    df_ta009 = clean_index(df_ta009)

    df_ta061 = change_col_type_to_string(df=df_ta061,
                                         col_name="N_PRT_TRC_ID_IN")

    df_ta061 = clean_index(df_ta061)

    # DataFrame for rows where Rule Applied
    df_rule_applied = filter_on_value_in_column(df=df_ta009,
                                                col_name="RULE_APPLIED",
                                                row_value="X")

    # Do the magic
    final_df, updated_parts_list = iterate_on_two_dataframes(df_input=df_rule_applied,
                                                             df_target=df_ta061)

    final_df.to_csv("TA061_TA002_ORA QA Updated.csv")

    # Save log file
    updated_df = pd.DataFrame(updated_parts_list, columns=["N_PRT_TRC_ID"])
    updated_df.to_csv("UpdatedParts.csv", index=False)
