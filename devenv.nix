{ config, pkgs, lib, ... }:
{
  languages.java = {
    enable = true;
    jdk.package = pkgs.jdk17_headless;
  };
  languages.python = {
    enable = true;
    uv.enable = true;
    uv.sync.enable = true;
  };

  env.LD_LIBRARY_PATH = lib.makeLibraryPath [ pkgs.zlib pkgs.stdenv.cc.cc.lib ];
  env.LAKE_PROJECT_DIR = config.devenv.root;
  env.LAKE_DATA_DIR = "${config.devenv.root}/data";
  env.SPARK_DRIVER_MEMORY = "6g";
}
