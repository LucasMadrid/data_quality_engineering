from pydeequ.checks import *
from pydeequ.verification import *


def validate_data(df):

    """
    Perform data validation with PyDeequ on the Spark DataFrame. 
    More info: https://github.com/awslabs/python-deequ

    Parameters
    ----------
    df: Spark DataFrame 
        Dataset cleaned and ready to be validated.

    Returns
    -------
    None

    """

    check = Check(spark, CheckLevel.Warning, "Review Check")
    checkResult_votes_column = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            #Votes
            check.isNonNegative("votes") \
            .hasDataType("votes", ConstrainableDataTypes.Numeric) \
            .isComplete("votes")
            .hasMin("votes", lambda x: x >= 0)
            #Phone
            .hasDataType("phone", ConstrainableDataTypes.Numeric) \
            .isComplete("phone") \
            .isNonNegative("phone") \
            #Location
            .hasDataType("location", ConstrainableDataTypes.String) \
            .isComplete("location") \
            #Name
            .hasDataType("name", ConstrainableDataTypes.String) \
            .isComplete("name")
        
            ) \
        .run()
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult_votes_column)
    checkResult_df.show(truncate=True)