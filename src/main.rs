use std::{env, process::exit, time::Duration, time::SystemTime};

use tokio::time::sleep;
use tokio_postgres::{Error, NoTls};
use uuid::Uuid;

pub struct TestPlayer {
    pub player_id: String,
    pub app_id: String,
    pub name: String,
}

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    // Connect to the database.

    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        println!("arguments are incorrect: pg-backfill <shard> <replica-ip> <primary-ip>");
        exit(1);
    }
    let shard = &args[1];
    let replica_ip = &args[2];
    let primary_ip = &args[3];
    let user = env::var("PG_USER").unwrap();
    let pass = env::var("PG_PASS").unwrap();

    //REPLICA
    let replica_connect_string = format!("postgres://{}:{}@{}/gamethrive", user, pass, replica_ip);
    let (replica, replica_connection) =
        tokio_postgres::connect(&replica_connect_string, NoTls).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = replica_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // PRIMARY
    let primary_connect_string = format!("postgres://{}:{}@{}/gamethrive", user, pass, primary_ip);
    let (primary, primary_connection) =
        tokio_postgres::connect(&primary_connect_string, NoTls).await?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = primary_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let query = "SELECT player_id, app_id, name from test_players WHERE created_at <= '2023-08-05'";
    //let query = "SELECT player_id, app_id, name from test_players WHERE created_at >= '2023-08-05' AND player_id = '6d4b3e44-a010-452b-8b7a-628340a629b0'";

    let statement = replica.prepare(query).await?;
    println!("{}", query);
    let rows = replica.query(&statement, &[]).await?;
    if rows.len() == 0 {
        println!("done");
        exit(1);
    } else {
        //construct comma separated string
        // construct Vec of TestPlayers
        let mut test_players = vec![];
        for row in rows {
            let player_id: Uuid = row.get(0);
            let app_id: Uuid = row.get(1);
            let value: Option<String> = row.get(2);
            match value {
                None => {
                    let test_player = TestPlayer {
                        player_id: player_id.to_string(),
                        app_id: app_id.to_string(),
                        name: " ".to_string(),
                    };
                    test_players.push(test_player);
                }
                Some(name) => {
                    let test_player = TestPlayer {
                        player_id: player_id.to_string(),
                        app_id: app_id.to_string(),
                        name: name
                            .to_string()
                            .replace("\'", "\'\'")
                            .to_string()
                            .replace("\"", "\\\"")
                            .to_string(),
                    };
                    test_players.push(test_player);
                }
            }
        }

        //update
        let mut update_count = 0;
        let mut player_count = 0;
        let now = SystemTime::now();
        println!("{} rows left to go on {}", test_players.len(), shard);
        while !test_players.is_empty() {
            let player = test_players.pop().unwrap();
            let update_query: String = format!(
                "UPDATE players \
                SET \"test_subscription_name\"=LEFT('{}', 255) \
                WHERE \"id\" = '{}' \
                AND \"app_id\" = '{}' \
                AND \"test_subscription_name\" IS NULL",
                player.name, player.player_id, player.app_id
            );
            //println!("{}", update_query);
            let statement = primary.prepare(&update_query).await?;
            let updated = primary.execute(&statement, &[]).await?;
            update_count = update_count + updated;
            player_count = player_count + 1;

            if player_count % 1000 == 0 {
                println!("updated {} rows so far..", update_count);
                println!("{} rows left to go on {}", test_players.len(), shard);
                match now.elapsed() {
                    Ok(elapsed) => {
                        // it prints '2'
                        println!("{} seconds", elapsed.as_secs());
                    }
                    Err(e) => {
                        // an error occurred!
                        println!("Error: {e:?}");
                    }
                }
                println!("sleeping for 15s");
                sleep(Duration::from_secs(15)).await;
            }
        }
        println!("total test_players processed: {}", player_count)
    }
    Ok(())
}
