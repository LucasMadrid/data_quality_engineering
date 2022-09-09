from pydeequ.checks import *
from pydeequ.verification import *


def validate_data(df): 
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