use polars::prelude::*;

// https://github.com/pola-rs/polars

fn main() -> anyhow::Result<()> {
    
    let model = Series::new("Model", &[
        "iPhone X",
        "iPhone XS",
        "iPhone 12",
        "iPhone 13",
        "Samsung S11",
        "Samsung S12",
        "Mi A1",
        "Mi A2"]);
                   


    let df = df!(
        "Model" => model,
        "Sales" => &[80,170,130,205,400,30,14,8],
        "Company" => &[
            "Apple",
            "Apple",
            "Apple",
            "Apple",
            "Samsung",
            "Samsung",
            "Xiao Mi",
            "Xiao Mi"
        ],
    )?;

//    get_info(&df)?;
//    filter_column(&df)?;
//    get_row(&df)?;
//    get_columns(&df)?;

//    let result = filter(&df)?;
//    aggregations(&result)?;

    Ok(())
}

fn get_info(df: &DataFrame) -> anyhow::Result<()> {

    println!("{:?}", df);
//    println!("{:?}", df.info());
    println!("{:?}", df.shape());
    println!("{:?}", df.dtypes());

    Ok(())
}

fn filter_column(df: &DataFrame) -> anyhow::Result<()> {

    df.columns(&["Model", "Sales"])?.iter().for_each(|&s| println!("{:?}", s.get(3)));
    println!("{:?}", &df[2].get(3)?);
    
    Ok(())
}
fn get_row(df: &DataFrame) -> anyhow::Result<()> {

    println!("{:?}", df.get_row(3));

    Ok(())
}
fn get_columns(df: &DataFrame) -> anyhow::Result<()> {

    // rearrangement of columns
    println!("{:?}", df.get_columns());
    println!("{:?}", df.select(&["Model", "Company", "Sales"])?);
    //println!("{:?}", df.select(dtype_cols(&[DataType::Int32])));
    // println!("{:?}", df.filter(column!(""))?);
    println!("{:?}", select_cols_by_type(df, DataType::Utf8)?);

    Ok(())
}
fn select_cols_by_type(df: &DataFrame, dtype: DataType) -> anyhow::Result<DataFrame> {
    let mut selected_cols: Vec<Field> = vec![];
    let schema = df.schema();

    for (data_type, field) in schema.iter_dtypes().zip(schema.iter_fields()) {
        if data_type == &dtype {
            selected_cols.push(field);
        }
    }

    Ok(df.select(&selected_cols.iter().map(|f| f.name().to_string()).collect::<Vec<String>>())?)
}
fn filter(df: &DataFrame) -> anyhow::Result<DataFrame> {

    let mask = df.column("Sales")?.gt(100)?;
    let filtered_df = df.filter(&mask)?;
    println!("{:?}", filtered_df);

    Ok(filtered_df)
}
fn aggregations(df: &DataFrame) -> anyhow::Result<()> {

    let result: i32 = df.column("Sales")?.sum().unwrap();
    println!("{:?}", result);

    let result: Vec<i32> = df.columns(&["Sales", "Sales"])?.iter().map(|s| s.sum().unwrap()).collect();
    println!("{:?}", result);

    let result = df.groupby(&["Company"])?.select(&["Sales"]).sum()?;
    println!("{:?}", result);

    let result = df.clone().lazy().groupby_stable([col("Company")]).agg([col("Sales").sum()]).collect()?;
    println!("{:?}", result);

    Ok(())
}
fn dummy(df: &DataFrame) -> anyhow::Result<()> {

    Ok(())
}
