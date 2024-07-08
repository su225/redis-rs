use strum_macros::EnumString;

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum DeploymentMode {
    Standalone,
    Replication,
    Cluster,
    Sentinel,
}

#[derive(Debug, Eq, PartialEq, EnumString)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum NodeRole {
    Master,
    Slave,
    Sentinel,
}
