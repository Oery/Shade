# Home Manager module: `services.shade.enable = true;` installs Shade for
# this user and runs `shaded` as a user service, with a daily backup.
#
# `settings` writes ~/.config/shade/config.toml (see config.example.toml);
# leave it empty to manage that file yourself. Secrets never go in
# `settings`, which ends up in the Nix store: the config names environment
# variables, and their values go in ~/.config/shade/.env (chmod 600).
self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.shade;
  toml = pkgs.formats.toml { };
  configFile = toml.generate "shade-config.toml" cfg.settings;

  backup = pkgs.writeShellScript "shade-backup" ''
    set -eu
    dir="''${XDG_STATE_HOME:-$HOME/.local/state}/shade/backups"
    ${pkgs.coreutils}/bin/mkdir -p "$dir"
    ${cfg.package}/bin/shade backup "$dir/shade-$(${pkgs.coreutils}/bin/date +%Y%m%d-%H%M%S).db"
    ${pkgs.findutils}/bin/find "$dir" -name 'shade-*.db' -mtime +14 -delete
  '';
in
{
  options.services.shade = {
    enable = lib.mkEnableOption "Shade, a personal agent daemon, as a user service";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      description = "The Shade package to use.";
    };

    settings = lib.mkOption {
      type = toml.type;
      default = { };
      example = lib.literalExpression ''
        {
          timezone = "Europe/Paris";
          model = {
            provider = "anthropic";
            id = "claude-opus-5-5";
            api_key_env = "ANTHROPIC_API_KEY";
            context_window = 1000000;
            max_output_tokens = 128000;
          };
          chat.caps = [ "now" "notify" ];
        }
      '';
      description = ''
        Shade's config.toml as Nix. Empty leaves ~/.config/shade/config.toml
        to you. Never put secrets here: name variables and set them in
        ~/.config/shade/.env.
      '';
    };

    backups = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Back up Shade's database daily, keeping 14 days.";
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [ cfg.package ];

    xdg.configFile."shade/config.toml" = lib.mkIf (cfg.settings != { }) { source = configFile; };

    systemd.user.services.shaded = {
      Unit = {
        Description = "Shade, a personal agent daemon";
        X-Restart-Triggers = lib.optional (cfg.settings != { }) "${configFile}";
      };

      Service = {
        Type = "exec";
        ExecStart = "${cfg.package}/bin/shaded";
        # SIGTERM shuts down gracefully; after this long, what is left
        # (MCP servers, sandboxed commands) is killed.
        KillMode = "mixed";
        TimeoutStopSec = 30;
        Restart = "on-failure";
        RestartSec = 10;
        UMask = "0077";
        # The shell tool runs commands in bubblewrap, found on PATH; the
        # usual profile and system directories follow, for MCP servers.
        Environment = [
          "RUST_LOG=info"
          "PATH=${lib.makeBinPath [ pkgs.bubblewrap ]}:%h/.nix-profile/bin:/etc/profiles/per-user/%u/bin:/run/current-system/sw/bin:/usr/local/bin:/usr/bin:/bin"
        ];
      };

      Install.WantedBy = [ "default.target" ];
    };

    systemd.user.services.shade-backup = lib.mkIf cfg.backups {
      Unit = {
        Description = "Back up Shade's database";
        Requisite = [ "shaded.service" ];
        After = [ "shaded.service" ];
      };

      Service = {
        Type = "oneshot";
        UMask = "0077";
        ExecStart = "${backup}";
      };
    };

    systemd.user.timers.shade-backup = lib.mkIf cfg.backups {
      Unit.Description = "Daily backup of Shade's database";

      Timer = {
        OnCalendar = "daily";
        RandomizedDelaySec = "15min";
        Persistent = true;
      };

      Install.WantedBy = [ "timers.target" ];
    };
  };
}
