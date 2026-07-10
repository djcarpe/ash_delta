defmodule DeltaDemo.Domain do
  use Ash.Domain, extensions: [AshGraphql.Domain]

  resources do
    resource DeltaDemo.Event
  end
end
