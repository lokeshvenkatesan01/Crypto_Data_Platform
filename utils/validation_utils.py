from utils.logger import get_logger

logger = get_logger(__name__)


def validate_not_empty(df, dataset_name="dataset"):
    """
    Ensure dataframe is not empty
    """

    if df.empty:
        raise ValueError(f"{dataset_name} is empty")

    logger.info(f"{dataset_name} row count: {len(df)}")


def validate_no_nulls(df, columns):
    """
    Ensure important columns contain no NULL values
    """

    for col in columns:

        null_count = df[col].isnull().sum()

        if null_count > 0:
            raise ValueError(f"Column {col} contains {null_count} null values")

    logger.info("Null validation passed")


def validate_positive_values(df, columns):
    """
    Ensure numeric columns contain positive values
    """

    for col in columns:

        invalid_count = (df[col] <= 0).sum()

        if invalid_count > 0:
            raise ValueError(
                f"Column {col} contains {invalid_count} invalid values"
            )

    logger.info("Positive value validation passed")