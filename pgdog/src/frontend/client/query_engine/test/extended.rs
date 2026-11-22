use crate::{
    expect_message,
    net::{
        BindComplete, CommandComplete, DataRow, ParameterDescription, Parameters, ParseComplete,
        ReadyForQuery, RowDescription,
    },
};

use super::prelude::*;

/// A text parameter's binary payload is still string bytes, even when SQL
/// casts the parameter to an integer shard key (issue #1668).
#[tokio::test]
async fn text_parameter_cast_to_bigint_routes_in_both_wire_formats() {
    use crate::net::Format;

    let mut client = TestClient::new_sharded(Parameters::default()).await;
    for (name, format) in [
        ("text_key", Format::Text),
        ("binary_text_key", Format::Binary),
    ] {
        client
            .send(
                Parse::named(name, "SELECT count(*) FROM sharded WHERE id = $1::bigint")
                    .with_data_types(&[25]), // Declare $1 as text, not bigint.
            )
            .await;
        client
            .send(Bind::new_params_codes(
                name,
                &[Parameter::new(b"0")],
                &[format],
            ))
            .await;
        client.send(Execute::new()).await;
        client.send(Sync).await;
        client.try_process().await.expect("process client messages");

        let result = client.read_until('Z').await;
        assert!(
            result.is_ok(),
            "text-typed key 0 in {format:?} format must succeed: {result:?}"
        );
        let messages = result.expect("query succeeded");
        assert!(
            messages.iter().any(|message| message.code() == 'D'),
            "count query should return a row in {format:?} format"
        );
    }
}

/// Test extended protocol with named prepared statement, parameter binding, and Describe.
#[tokio::test]
async fn test_extended_with_params_and_describe() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    client.send(Parse::named("test", "SELECT $1")).await;
    client
        .send(Bind::new_params(
            "test",
            &[Parameter {
                len: 3,
                data: "123".into(),
            }],
        ))
        .await;
    client.send(Describe::new_statement("test")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete); // '1'
    expect_message!(client.read().await, BindComplete); // '2'
    expect_message!(client.read().await, ParameterDescription); // 't'
    expect_message!(client.read().await, RowDescription); // 'T'
    let row = expect_message!(client.read().await, DataRow); // 'D'
    assert_eq!(
        row.get::<String>(0, crate::net::Format::Text),
        Some("123".into())
    );
    expect_message!(client.read().await, CommandComplete); // 'C'
    let rfq = expect_message!(client.read().await, ReadyForQuery); // 'Z'
    assert_eq!(rfq.status, 'I');
}
