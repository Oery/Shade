# NixOS module: `services.shade.enable = true;` installs Shade and starts
# `shaded` as a user service for every user who logs in (set
# `users.users.<name>.linger = true` to start it at boot). Each user keeps
# their own ~/.config/shade/config.toml and .env.
self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.shade;
in
{
  options.services.shade = {
    enable = lib.mkEnableOption "Shade, a personal agent daemon, as a systemd user service";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      description = "The Shade package to use.";
    };

    backups = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Back up each user's Shade database daily, keeping 14 days.";
    };
  };

  config = lib.mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];
    systemd.packages = [ cfg.package ];

    systemd.user.services.shaded = {
      wantedBy = [ "default.target" ];
      # The shell tool runs commands in bubblewrap, found on PATH.
      path = [ pkgs.bubblewrap ];
    };

    systemd.user.services.shade-backup.path = [ pkgs.coreutils pkgs.findutils ];
    systemd.user.timers.shade-backup.wantedBy = lib.mkIf cfg.backups [ "timers.target" ];
  };
}
