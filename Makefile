output_dir = $(CURDIR)/deploy
MAVEN = mvn


install: all
	mkdir -p $(output_dir)/lib 
	cp $(CURDIR)/*/target/qlikglue*.jar $(output_dir)/lib
	cp $(CURDIR)/*/*/target/qlikglue*.jar $(output_dir)/lib
	#cp -u $(CURDIR)/dependencies/*.jar $(output_dir)/lib/dependencies
	#cp -u $(CURDIR)/*/*/target/dependencies/*.jar $(output_dir)/lib/dependencies
	#cp -R $(CURDIR)/*/target/apidocs $(output_dir)/doc
	#cp -R $(CURDIR)/*/*/target/apidocs $(output_dir)/doc

all: qlikglue.jar


.PHONY: check-env

qlikglue.jar:  .PHONY
	$(MAVEN) package -Dqlikglue.VERSION=$(GGBD_VERSION) -Dqlikglue.HOME=$(GGBD_HOME)

docs: .PHONY
	$(MAVEN) javadoc:aggregate -Dqlikglue.VERSION=$(GGBD_VERSION) -Dqlikglue.HOME=$(GGBD_HOME)


clean: .PHONY
	$(MAVEN) clean
	rm -rf $(output_dir)
	rm -rf $(CURDIR)/dependencies

check-env:
