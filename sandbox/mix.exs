defmodule Sandbox.MixProject do
  use Mix.Project

  def project do
    [
      app: :sandbox,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:rusty_csv, path: ".."},
      {:nimble_csv, "~> 1.2"},
      {:csv, "~> 3.2"},
      {:benchee, "~> 1.0"},
      {:rustler, "~> 0.37"}
    ]
  end
end
