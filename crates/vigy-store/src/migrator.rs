//! Schema migrations. New columns / tables land here as numbered
//! migrations; `Store::open` runs `Migrator::up()` automatically.

use sea_orm_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20260520_000001_init::Migration),
            Box::new(m20260521_000002_vigy_kv::Migration),
        ]
    }
}

mod m20260521_000002_vigy_kv {
    use sea_orm_migration::prelude::*;

    pub struct Migration;
    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20260521_000002_vigy_kv"
        }
    }

    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .create_table(
                    Table::create()
                        .table(VigyKv::Table)
                        .if_not_exists()
                        .col(ColumnDef::new(VigyKv::VigyId).string().not_null())
                        .col(ColumnDef::new(VigyKv::Key).string().not_null())
                        .col(ColumnDef::new(VigyKv::ValueJson).text().not_null())
                        .col(ColumnDef::new(VigyKv::UpdatedAt).string().not_null())
                        .primary_key(
                            Index::create()
                                .col(VigyKv::VigyId)
                                .col(VigyKv::Key),
                        )
                        .to_owned(),
                )
                .await?;
            manager
                .create_index(
                    Index::create()
                        .name("idx_vigy_kv_vigy_id")
                        .table(VigyKv::Table)
                        .col(VigyKv::VigyId)
                        .to_owned(),
                )
                .await?;
            Ok(())
        }

        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .drop_table(Table::drop().table(VigyKv::Table).to_owned())
                .await?;
            Ok(())
        }
    }

    #[derive(Iden)]
    enum VigyKv {
        Table,
        VigyId,
        Key,
        ValueJson,
        UpdatedAt,
    }
}

mod m20260520_000001_init {
    use sea_orm_migration::prelude::*;

    pub struct Migration;

    impl MigrationName for Migration {
        fn name(&self) -> &str {
            "m20260520_000001_init"
        }
    }

    #[async_trait::async_trait]
    impl MigrationTrait for Migration {
        async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .create_table(
                    Table::create()
                        .table(Vigies::Table)
                        .if_not_exists()
                        .col(ColumnDef::new(Vigies::Id).string().not_null().primary_key())
                        .col(ColumnDef::new(Vigies::Name).string().not_null())
                        .col(ColumnDef::new(Vigies::Program).text().not_null())
                        .col(
                            ColumnDef::new(Vigies::TickIntervalMs)
                                .big_integer()
                                .not_null(),
                        )
                        .col(ColumnDef::new(Vigies::Enabled).boolean().not_null())
                        .col(ColumnDef::new(Vigies::CreatedAt).string().not_null())
                        .col(ColumnDef::new(Vigies::UpdatedAt).string().not_null())
                        .col(ColumnDef::new(Vigies::LabelsJson).text().not_null())
                        .to_owned(),
                )
                .await?;
            manager
                .create_table(
                    Table::create()
                        .table(VigyRuns::Table)
                        .if_not_exists()
                        .col(ColumnDef::new(VigyRuns::Id).string().not_null().primary_key())
                        .col(ColumnDef::new(VigyRuns::VigyId).string().not_null())
                        .col(ColumnDef::new(VigyRuns::StartedAt).string().not_null())
                        .col(ColumnDef::new(VigyRuns::EndedAt).string())
                        .col(ColumnDef::new(VigyRuns::Result).string().not_null())
                        .col(ColumnDef::new(VigyRuns::Error).text())
                        .col(ColumnDef::new(VigyRuns::ActionsJson).text().not_null())
                        .to_owned(),
                )
                .await?;
            manager
                .create_index(
                    Index::create()
                        .name("idx_vigy_runs_vigy_id")
                        .table(VigyRuns::Table)
                        .col(VigyRuns::VigyId)
                        .to_owned(),
                )
                .await?;
            manager
                .create_table(
                    Table::create()
                        .table(VigyEvents::Table)
                        .if_not_exists()
                        .col(
                            ColumnDef::new(VigyEvents::Seq)
                                .big_integer()
                                .not_null()
                                .primary_key()
                                .auto_increment(),
                        )
                        .col(ColumnDef::new(VigyEvents::VigyId).string().not_null())
                        .col(ColumnDef::new(VigyEvents::RunId).string().not_null())
                        .col(ColumnDef::new(VigyEvents::Kind).string().not_null())
                        .col(ColumnDef::new(VigyEvents::PayloadJson).text())
                        .col(ColumnDef::new(VigyEvents::Result).string())
                        .col(ColumnDef::new(VigyEvents::Message).text())
                        .col(ColumnDef::new(VigyEvents::EmittedAt).string().not_null())
                        .to_owned(),
                )
                .await?;
            manager
                .create_index(
                    Index::create()
                        .name("idx_vigy_events_vigy_id")
                        .table(VigyEvents::Table)
                        .col(VigyEvents::VigyId)
                        .to_owned(),
                )
                .await?;
            Ok(())
        }

        async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
                .drop_table(Table::drop().table(VigyEvents::Table).to_owned())
                .await?;
            manager
                .drop_table(Table::drop().table(VigyRuns::Table).to_owned())
                .await?;
            manager
                .drop_table(Table::drop().table(Vigies::Table).to_owned())
                .await?;
            Ok(())
        }
    }

    #[derive(Iden)]
    enum Vigies {
        Table,
        Id,
        Name,
        Program,
        TickIntervalMs,
        Enabled,
        CreatedAt,
        UpdatedAt,
        LabelsJson,
    }

    #[derive(Iden)]
    enum VigyRuns {
        Table,
        Id,
        VigyId,
        StartedAt,
        EndedAt,
        Result,
        Error,
        ActionsJson,
    }

    #[derive(Iden)]
    enum VigyEvents {
        Table,
        Seq,
        VigyId,
        RunId,
        Kind,
        PayloadJson,
        Result,
        Message,
        EmittedAt,
    }
}
