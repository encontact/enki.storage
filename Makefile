# To learn makefiles: https://makefiletutorial.com/
# On windows, use NMake: https://docs.microsoft.com/pt-br/cpp/build/reference/nmake-reference?view=msvc-160
dotnetFramework = net5.0
packFramework = netstandard2.0
solution = ./enki.storage.sln
libProject = ./src/enki.storage/enki.storage.csproj
nuspec = ./enki.storage.nuspec
distPath = ./dist
artifactDir = ./artifacts
nupkgFile = $(shell find ./artifacts -type f -name '*.nupkg')

show-pack:
	echo "${nupkgFile}"

run-clean: clean restore build

all : clean restore build

clean:
	dotnet clean ${solution}

restore:
	dotnet restore ${solution}

build:
	dotnet build -c Release ${solution}

run-test:
	dotnet test ${solution}

publish:
	dotnet publish ${libProject} -c Release -o out/Release/

pack:
	dotnet pack -c Release -o ${artifactDir} ${libProject}

push-pack:
	dotnet nuget push ${nupkgFile} --api-key ${NUGET_API} --source https://api.nuget.org/v3/index.json

# Mais em: https://renatogroffe.medium.com/net-nuget-atualizando-packages-via-linha-de-comando-b0c6b596ed2
# Para instalar dependência: dotnet tool install --global dotnet-outdated-tool
update-dependencies:
	dotnet-outdated -u:Prompt ${solution}

# Mais em: https://devblogs.microsoft.com/nuget/how-to-scan-nuget-packages-for-security-vulnerabilities/
check-vulnerabilities:
	dotnet list package --vulnerable

# Detalhes de versionamento em:
# https://github.com/dotnet/Nerdbank.GitVersioning/blob/master/doc/nbgv-cli.md
prepare-release:
	nbgv prepare-release

# Mesmo após gerar a Tag é necessário enviar a tag para o servidor
tag-release:
	nbgv tag