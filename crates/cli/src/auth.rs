//! Bearer tokens for the control plane: the deploy token kept per server in the
//! OS credential store, and tokens taken from the environment.
use crate::args::{AuthCmd, AuthCommand, AuthLoginCmd};
use common::first_non_empty_trimmed;
use std::env;

const KEYRING_SERVICE: &str = "dd";
const KEYRING_DEPLOY_TOKEN_PREFIX: &str = "deploy-token:";

pub fn run(command: AuthCmd, server: &str) -> Result<(), String> {
    match command.command {
        AuthCommand::Login(command) => auth_login(server, command),
        AuthCommand::Logout => auth_logout(server),
        AuthCommand::Status => auth_status(server),
    }
}

fn auth_login(server: &str, command: AuthLoginCmd) -> Result<(), String> {
    let token = match command.token {
        Some(token) => token,
        None => rpassword::prompt_password("Deploy token: ").map_err(|error| error.to_string())?,
    };
    let token = token.trim();
    if token.is_empty() {
        return Err("deploy token must not be empty".to_string());
    }

    keyring_entry(server)?
        .set_password(token)
        .map_err(|error| {
            format!("failed to store deploy token for {server} in the OS credential store: {error}")
        })?;
    println!("Stored deploy token for {server}");
    Ok(())
}

fn auth_logout(server: &str) -> Result<(), String> {
    match keyring_entry(server)?.delete_credential() {
        Ok(()) => println!("Removed deploy token for {server}"),
        Err(keyring::Error::NoEntry) => println!("No deploy token stored for {server}"),
        Err(error) => {
            return Err(format!(
                "failed to remove deploy token for {server} from the OS credential store: {error}"
            ));
        }
    }
    Ok(())
}

fn auth_status(server: &str) -> Result<(), String> {
    match stored_deploy_token(server) {
        Ok(Some(_)) => println!("Deploy token is stored for {server}"),
        Ok(None) => println!("No deploy token stored for {server}"),
        Err(error) => return Err(error),
    }
    Ok(())
}

pub fn private_bearer_token() -> Option<String> {
    first_non_empty_trimmed([
        env::var("DD_PRIVATE_TOKEN").unwrap_or_default(),
        env::var("PRIVATE_BEARER_TOKEN").unwrap_or_default(),
    ])
}

pub fn deploy_bearer_token(server: &str) -> Option<String> {
    first_non_empty_trimmed([
        env::var("DD_TOKEN").unwrap_or_default(),
        env::var("DD_DEPLOY_TOKEN").unwrap_or_default(),
    ])
    .or_else(|| stored_deploy_token(server).ok().flatten())
}

fn stored_deploy_token(server: &str) -> Result<Option<String>, String> {
    match keyring_entry(server)?.get_password() {
        Ok(token) => Ok(first_non_empty_trimmed([token])),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(format!(
            "failed to read deploy token for {server} from the OS credential store: {error}"
        )),
    }
}

fn keyring_entry(server: &str) -> Result<keyring::Entry, String> {
    keyring::Entry::new(KEYRING_SERVICE, &keyring_account(server))
        .map_err(|error| format!("failed to open OS credential store: {error}"))
}

fn keyring_account(server: &str) -> String {
    format!(
        "{KEYRING_DEPLOY_TOKEN_PREFIX}{}",
        server.trim_end_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::keyring_account;

    #[test]
    fn stored_deploy_token_key_is_scoped_to_base_url() {
        assert_eq!(
            keyring_account("https://dd.example.com/"),
            "deploy-token:https://dd.example.com"
        );
        assert_ne!(
            keyring_account("https://dd.example.com"),
            keyring_account("https://other.example.com")
        );
    }
}
