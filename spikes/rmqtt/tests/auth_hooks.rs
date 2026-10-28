mod common;

use common::*;
use rmqtt_spike::Event;
use rumqttc::ConnectReturnCode;

#[tokio::test]
async fn auth_hooks() {
    let server = start().await;

    let (client, eventloop, session_present) =
        connect_ok(&server, DEVICE_A, DEVICE_A, TOKEN_A, true).await;
    assert!(!session_present);

    let events = server.events().await;
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Connect {
            client_id,
            username: Some(username),
            proto_ver: 4,
            clean_session: true,
        } if client_id == DEVICE_A && username == DEVICE_A
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Authenticate {
            client_id,
            username: Some(username),
            password_ok: true,
            proto_ver: 4,
            clean_session: true,
        } if client_id == DEVICE_A && username == DEVICE_A
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Connected {
            client_id,
            username: Some(username),
            session_present: false,
            ..
        } if client_id == DEVICE_A && username == DEVICE_A
    )));

    assert_eq!(
        connect_refused(&server, DEVICE_A, DEVICE_A, "wrong-token").await,
        ConnectReturnCode::BadUserNamePassword
    );
    assert_eq!(
        server.sessions().await,
        1,
        "a refused connect must not create a session"
    );

    assert_eq!(
        connect_refused(&server, DEVICE_B, DEVICE_A, TOKEN_A).await,
        ConnectReturnCode::BadUserNamePassword
    );
    let events = server.events().await;
    assert!(
        events.iter().any(|event| matches!(
            event,
            Event::Authenticate {
                client_id,
                username: Some(username),
                password_ok: true,
                ..
            } if client_id == DEVICE_B && username == DEVICE_A
        )),
        "hook must see the credential belongs to A while the client id is B"
    );

    drop(client);
    drop(eventloop);
    wait_event(
        &server,
        |event| matches!(event, Event::Disconnected { client_id, .. } if client_id == DEVICE_A),
        std::time::Duration::from_secs(5),
    )
    .await;
    wait_sessions(&server, 0, std::time::Duration::from_secs(5)).await;
}
