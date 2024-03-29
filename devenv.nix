{ pkgs, ... }:

{
  # https://devenv.sh/packages/
  packages = [
    pkgs.git
    pkgs.zlib
    pkgs.stdenv.cc.cc.lib
    (pkgs.python311.withPackages (ps: with ps; [ pip numpy ]))
  ];

  languages.python = {
    enable = true;
    version = "3.11.6";
    venv = {
      enable = true;
      requirements = ''
        pyarrow
        pandas[excel]==2.2.0
        gspread
        thefuzz
      '';
    };
  };

  enterShell = ''
    fish
  '';
}
